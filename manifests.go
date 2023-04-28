package kubeyamlutils

import (
	"bytes"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type Manifests struct {
	logger       logr.Logger
	objects      []*unstructured.Unstructured
	sorted       bool
	reverseOrder bool
}

const (
	// 原始yaml的json序列化.
	originObject = "github.com.klzsysy.kubeyamlutils/original"
	// apply到集群后返回的对象json序列化的sha1.
	applyResultObjectSHA1Sum = "github.com.klzsysy.kubeyamlutils/sha1sum"

	coreV1Group = "v1"
	serviceKind = "ServiceAccount"
)

func (m *Manifests) GetObjects() []client.Object {
	objects := make([]client.Object, len(m.objects))
	for index, obj := range m.objects {
		objects[index] = obj.DeepCopy()
	}
	return objects
}

func (m *Manifests) SetLogger(logger logr.Logger) {
	m.logger = logger
}

func (m *Manifests) calculateSHA1Sum(origin, currentObject *unstructured.Unstructured) string {
	deepCopy := currentObject.DeepCopy()
	// 删除这些在对象apply到集群之后会容易变动或者容易被系统修改的字段，不计算校验和
	// 即在目标集群修改这些字段字段不会触发更新
	unstructured.RemoveNestedField(deepCopy.Object, "status")
	unstructured.RemoveNestedField(deepCopy.Object, "metadata", "managedFields")
	unstructured.RemoveNestedField(deepCopy.Object, "metadata", "resourceVersion")
	unstructured.RemoveNestedField(deepCopy.Object, "metadata", "generation")
	unstructured.RemoveNestedField(deepCopy.Object, "metadata", "finalizers")
	unstructured.RemoveNestedField(deepCopy.Object, "metadata", "ownerReferences")
	// 许多系统控制器会在annotation中加入控制字段，如 "deployment.kubernetes.io/revision"
	unstructured.RemoveNestedField(deepCopy.Object, "metadata", "annotations")
	if origin.GetGenerateName() != "" {
		// 如果name是生成的，将忽略
		unstructured.RemoveNestedField(deepCopy.Object, "metadata", "name")
	}
	// 在小于等于1.23的k8s版本中Secret中的 `secrets` 字段是后置生成填充的特例，需要忽略
	if deepCopy.GetAPIVersion() == coreV1Group && deepCopy.GetKind() == serviceKind {
		unstructured.RemoveNestedField(deepCopy.Object, "secrets")
	}

	// 确保原始对象中的annotation是不被忽略的
	// 如果需要可加入原始对象中的Status部分
	originAnn := origin.GetAnnotations()
	if originAnn != nil {
		deepCopy.SetAnnotations(originAnn)
	}
	body, _ := deepCopy.MarshalJSON()
	return fmt.Sprintf("%x", sha1.Sum(body)) //nolint:gosec
}

// makeDiffAnnotation 把除了status部分的sha1和原始对象body写入对象的annotation.
func (m *Manifests) makeDiffAnnotation(ctx context.Context, c client.Client, origin, createResult *unstructured.Unstructured) error {
	appOriginBody, _ := origin.MarshalJSON()
	createResultCopy := createResult.DeepCopy()

	ann := createResult.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	// 原始object作为输入内容
	ann[originObject] = string(appOriginBody)
	// 输出object内容的非status部分的sha1并写入annotation
	ann[applyResultObjectSHA1Sum] = m.calculateSHA1Sum(origin, createResult)
	createResultCopy.SetAnnotations(ann)
	return c.Patch(ctx, createResultCopy, client.MergeFrom(createResult))
}

func (m *Manifests) updateObject(ctx context.Context, c client.Client, origin, current *unstructured.Unstructured) error {
	updateResult := origin.DeepCopy()
	updateResult.SetResourceVersion(current.GetResourceVersion())
	if err := c.Update(ctx, updateResult); err != nil {
		return err
	}
	// 根据返回结果计算
	if err := m.makeDiffAnnotation(ctx, c, origin, updateResult); err != nil {
		return err
	}
	m.logger.Info("object updated",
		"apiVersion", updateResult.GetAPIVersion(), "kind", updateResult.GetKind(),
		"namespace", updateResult.GetNamespace(), "name", updateResult.GetName())
	return nil
}

// Apply create/update object to client cluster.
func (m *Manifests) Apply(ctx context.Context, c client.Client) error {
	if m.sorted && m.reverseOrder {
		sort.SliceStable(m.objects, func(i, j int) bool {
			return i > j
		})
		m.reverseOrder = false
	}
	for _, obj := range m.objects {
		objShadow := obj.DeepCopy()
		if _, err := controllerutil.CreateOrUpdate(ctx, c, obj, func() error {
			version := obj.GetResourceVersion()
			obj.Object = objShadow.Object
			obj.SetResourceVersion(version)
			return nil
		}); err != nil {
			return nil
		}
	}
	return nil
}

// Sync 将object create/update到目标cluster，如果已经apply过且源和目标均无修改将跳过更新，避免无意义的操作
// 目标集群中对象Annotation的修改将被忽略.
func (m *Manifests) Sync(ctx context.Context, c client.Client) error {
	if m.sorted && m.reverseOrder {
		sort.SliceStable(m.objects, func(i, j int) bool {
			return i > j
		})
		m.reverseOrder = false
	}
	for _, obj := range m.objects {
		result := obj.DeepCopy()
		if err := c.Get(ctx, client.ObjectKeyFromObject(obj), result); err != nil {
			if apierrors.IsNotFound(err) {
				result = obj.DeepCopy()
				if err = c.Create(ctx, result); err != nil {
					return err
				}
				if err = m.makeDiffAnnotation(ctx, c, obj, result); err != nil {
					return err
				}
				m.logger.Info("object created",
					"apiVersion", result.GetAPIVersion(), "kind", result.GetKind(),
					"namespace", result.GetNamespace(), "name", result.GetName())
				continue
			} else {
				return err
			}
		}

		ann := result.GetAnnotations()
		if ann == nil || ann[originObject] == "" || ann[applyResultObjectSHA1Sum] == "" {
			if err := m.updateObject(ctx, c, obj, result); err != nil {
				return err
			}
			continue
		}
		recordOriginBody := ann[originObject]
		recordSHA1sum := ann[applyResultObjectSHA1Sum]

		originBody, _ := obj.MarshalJSON()
		currentSHA1Sum := m.calculateSHA1Sum(obj, result)
		// recordOriginBody 和 originBody 不同，表示源被更新
		// recordSHA1sum 和 currentSHA1Sum 不同，表示目标被修改
		if recordOriginBody == string(originBody) && recordSHA1sum == currentSHA1Sum {
			// 相同表示源和目标都没有被修改，不需要更新对象
			m.logger.V(3).Info("object is synced",
				"apiVersion", result.GetAPIVersion(), "kind", result.GetKind(),
				"namespace", result.GetNamespace(), "name", result.GetName())
			continue
		}
		// diff 检查失败，更新对象
		if err := m.updateObject(ctx, c, obj, result); err != nil {
			return err
		}
		continue
	}
	return nil
}

func (m *Manifests) Delete(ctx context.Context, client client.Client) error {
	if m.sorted && !m.reverseOrder {
		sort.SliceStable(m.objects, func(i, j int) bool {
			return i > j
		})
		m.reverseOrder = true
	}
	for _, obj := range m.objects {
		if err := client.Delete(ctx, obj); err != nil {
			// 对象已经被删除或者CRD已经不存在
			if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
				m.logger.V(3).Info("object do not exist already",
					"apiVersion", obj.GetAPIVersion(), "kind", obj.GetKind(),
					"namespace", obj.GetNamespace(), "name", obj.GetName())
				continue
			}
			return err
		}
		m.logger.Info("object deleted",
			"apiVersion", obj.GetAPIVersion(), "kind", obj.GetKind(),
			"namespace", obj.GetNamespace(), "name", obj.GetName())
	}
	return nil
}

func (m *Manifests) MutateObject(mutateFn func(unstructured2 *unstructured.Unstructured) error) error {
	for _, obj := range m.objects {
		if err := mutateFn(obj); err != nil {
			return err
		}
	}
	return nil
}

type Option func(parse *ManifestsParser)

// SetNamespace sets namespace for objects.
func SetNamespace(mapper meta.RESTMapper, namespace string) Option {
	return func(parse *ManifestsParser) {
		parse.mapper = mapper
		parse.namespace = namespace
	}
}

// SortObject sorts objects in the order of CRDs, cluster scoped objects, namespaces, namespace scoped objects.
func SortObject(mapper meta.RESTMapper) Option {
	return func(parse *ManifestsParser) {
		parse.mapper = mapper
		parse.sort = true
	}
}

// ReverseSortObject sorts objects in the order of namespace scoped objects, namespaces, cluster scoped objects, CRDs.
func ReverseSortObject(mapper meta.RESTMapper) Option {
	return func(parse *ManifestsParser) {
		parse.mapper = mapper
		parse.reverseOrder = true
		parse.sort = true
	}
}

// WithLogger add logger.
func WithLogger(logger logr.Logger) Option {
	return func(parse *ManifestsParser) {
		parse.logger = logger
	}
}

type ManifestsParser struct {
	logger       logr.Logger
	directory    string
	files        []string
	raws         [][]byte
	mapper       meta.RESTMapper
	namespace    string
	reverseOrder bool
	sort         bool
}

// NewManifestsParserFromFile reads manifests from a file.
func NewManifestsParserFromFile(file string, opts ...Option) *ManifestsParser {
	p := &ManifestsParser{
		logger: logr.Discard(),
		files:  []string{file},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// NewManifestsParserFromDirectory reads manifests from a directory.
// flow kubectl recognized file extensions are [.json .yaml .yml].
func NewManifestsParserFromDirectory(directory string, opts ...Option) *ManifestsParser {
	p := &ManifestsParser{
		logger:    logr.Discard(),
		directory: directory,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// NewManifestsParser reads manifests from a byte array.
func NewManifestsParser(bytes []byte, opts ...Option) *ManifestsParser {
	p := &ManifestsParser{
		logger: logr.Discard(),
		raws:   [][]byte{bytes},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *ManifestsParser) isYAMLOrJSON(name string) bool {
	for _, suffix := range []string{"yaml", "yml", "json"} {
		if strings.HasSuffix(name, "."+suffix) {
			return true
		}
	}
	return false
}

func (p *ManifestsParser) readFiles() error {
	if p.directory != "" {
		fss, err := os.ReadDir(filepath.Clean(p.directory))
		if err != nil {
			return err
		}
		for _, fs := range fss {
			if fs.IsDir() {
				continue
			}
			if !p.isYAMLOrJSON(fs.Name()) {
				continue
			}
			p.files = append(p.files, filepath.Join(p.directory, fs.Name()))
		}
	}
	if len(p.files) > 0 {
		for _, file := range p.files {
			body, err := os.ReadFile(filepath.Clean(file))
			if err != nil {
				return err
			}
			if string(body) == "" {
				continue
			}
			p.raws = append(p.raws, body)
		}
	}
	return nil
}

func (p *ManifestsParser) MustParse() *Manifests {
	m, err := p.Parse()
	if err != nil {
		panic(err)
	}
	return m
}

func (p *ManifestsParser) Parse() (*Manifests, error) {
	var err error
	if err = p.readFiles(); err != nil {
		return nil, err
	}

	manifests := &Manifests{}
	for _, raw := range p.raws {
		decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 100)
		for {
			var rawObj runtime.RawExtension
			if err = decoder.Decode(&rawObj); err != nil {
				break
			}
			if rawObj.Raw == nil && rawObj.Object == nil {
				continue
			}
			obj, gvk, err := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme).Decode(rawObj.Raw, nil, nil)
			if err != nil {
				return nil, err
			}
			unstructuredMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
			if err != nil {
				return nil, err
			}
			unstructuredObj := &unstructured.Unstructured{Object: unstructuredMap}

			if p.mapper != nil && p.namespace != "" {
				r, err := p.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
				if err != nil {
					return nil, err
				}
				if r.Scope.Name() == meta.RESTScopeNameNamespace {
					unstructuredObj.SetNamespace(p.namespace)
				}
			}
			manifests.objects = append(manifests.objects, unstructuredObj)
		}
		if err != io.EOF {
			return nil, err
		}
	}

	if err = p.sortObject(manifests); err != nil {
		return nil, errors.WithMessage(err, "sort manifest object fail")
	}
	manifests.logger = p.logger
	return manifests, nil
}

func (p *ManifestsParser) sortObject(manifests *Manifests) error {
	if !p.sort || len(manifests.objects) == 0 {
		return nil
	}
	if p.mapper == nil {
		return fmt.Errorf("restMapper is required for sorting objects")
	}

	crdObject := make([]*unstructured.Unstructured, 0)
	clusterObject := make([]*unstructured.Unstructured, 0)
	namespacedObj := make([]*unstructured.Unstructured, 0)
	sortedObj := make([]*unstructured.Unstructured, 0)

	for index := range manifests.objects {
		obj := manifests.objects[index]
		gvk := obj.GroupVersionKind()
		r, err := p.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err == nil {
			if gvk.Kind == "CustomResourceDefinition" {
				crdObject = append(crdObject, obj)
				continue
			}
			if r.Scope.Name() == meta.RESTScopeNameRoot {
				clusterObject = append(clusterObject, obj)
				continue
			}
			namespacedObj = append(namespacedObj, obj)
		} else {
			// Custom Resources
			if meta.IsNoMatchError(err) {
				if obj.GetNamespace() != "" {
					namespacedObj = append(namespacedObj, obj)
				} else {
					clusterObject = append(clusterObject, obj)
				}
			} else {
				return err
			}
		}
	}
	sortedObj = append(sortedObj, crdObject...)
	sortedObj = append(sortedObj, clusterObject...)
	sortedObj = append(sortedObj, namespacedObj...)
	if p.reverseOrder {
		sort.SliceStable(sortedObj, func(i, j int) bool {
			return i > j
		})
		manifests.reverseOrder = true
	}
	manifests.objects = sortedObj
	manifests.sorted = true
	return nil
}
