package kube

import (
	"context"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/cofy-x/kova/internal/observability"

	"go.opentelemetry.io/otel/attribute"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

type API interface {
	GetSecretData(ctx context.Context, namespace string, name string, key string) (string, error)
	PodExists(ctx context.Context, namespace string, name string) (bool, error)
	CreatePod(ctx context.Context, pod *corev1.Pod) error
	WaitPodReady(ctx context.Context, namespace string, name string, timeout time.Duration) error
	DeletePod(ctx context.Context, namespace string, name string) error
	WritePodLogsTail(ctx context.Context, namespace string, name string, tailLines int64, out io.Writer) error
	ListPods(ctx context.Context, namespace string, out io.Writer, wide bool) error
	ListPodsWithOptions(ctx context.Context, namespace string, out io.Writer, opts ListPodsOptions) error
	Exec(ctx context.Context, namespace string, pod string, opts ExecOptions) error
	ScaleDeployment(ctx context.Context, namespace string, name string, replicas int32) error
}

type ListPodsOptions struct {
	Wide          bool
	LabelSelector string
}

type ExecOptions struct {
	Command []string
	Stdin   io.Reader
	Stdout  io.Writer
	Stderr  io.Writer
	TTY     bool
}

type Client struct {
	config    *rest.Config
	clientset kubernetes.Interface
}

func NewClient(kubeconfig string) (*Client, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}
	return NewClientForConfig(config)
}

func NewInClusterClient() (*Client, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return NewClientForConfig(config)
}

func NewClientForConfig(config *rest.Config) (*Client, error) {
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &Client{config: config, clientset: clientset}, nil
}

func (k *Client) GetSecretData(ctx context.Context, namespace string, name string, key string) (string, error) {
	secret, err := k.clientset.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	value := secret.Data[key]
	if len(value) == 0 {
		return "", fmt.Errorf("secret field %s is empty", key)
	}
	return string(value), nil
}

func (k *Client) PodExists(ctx context.Context, namespace string, name string) (bool, error) {
	_, err := k.clientset.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return true, nil
	}
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return false, err
}

func (k *Client) CreatePod(ctx context.Context, pod *corev1.Pod) (err error) {
	ctx, op := kubeOperation(ctx, "create_pod", pod.Namespace, pod.Name)
	defer func() { op.End(err) }()
	_, err = k.clientset.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	return err
}

func (k *Client) WaitPodReady(ctx context.Context, namespace string, name string, timeout time.Duration) (err error) {
	ctx, op := kubeOperation(ctx, "wait_pod_ready", namespace, name)
	defer func() { op.End(err) }()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		pod, err := k.clientset.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err == nil && podReady(pod) {
			return nil
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("pod %s/%s did not become ready within %s", namespace, name, timeout)
		case <-ticker.C:
		}
	}
}

func (k *Client) DeletePod(ctx context.Context, namespace string, name string) (err error) {
	ctx, op := kubeOperation(ctx, "delete_pod", namespace, name)
	defer func() { op.End(err) }()
	err = k.clientset.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		_, err := k.clientset.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for pod %s/%s deletion", namespace, name)
		case <-ticker.C:
		}
	}
}

func (k *Client) WritePodLogsTail(ctx context.Context, namespace string, name string, tailLines int64, out io.Writer) (err error) {
	ctx, op := kubeOperation(ctx, "pod_logs", namespace, name)
	defer func() { op.End(err) }()
	stream, err := k.clientset.CoreV1().Pods(namespace).GetLogs(name, &corev1.PodLogOptions{TailLines: &tailLines}).Stream(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()
	_, err = io.Copy(out, stream)
	return err
}

func (k *Client) ListPods(ctx context.Context, namespace string, out io.Writer, wide bool) (err error) {
	return k.ListPodsWithOptions(ctx, namespace, out, ListPodsOptions{Wide: wide})
}

func (k *Client) ListPodsWithOptions(ctx context.Context, namespace string, out io.Writer, opts ListPodsOptions) (err error) {
	ctx, op := kubeOperation(ctx, "list_pods", namespace, "")
	defer func() { op.End(err) }()
	pods, err := k.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: opts.LabelSelector})
	if err != nil {
		return err
	}
	writer := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	if opts.Wide {
		fmt.Fprintln(writer, "NAME\tREADY\tSTATUS\tRESTARTS\tAGE\tIP\tNODE")
	} else {
		fmt.Fprintln(writer, "NAME\tREADY\tSTATUS\tRESTARTS\tAGE")
	}
	for _, pod := range pods.Items {
		ready, total, restarts := podContainerCounts(pod)
		age := time.Since(pod.CreationTimestamp.Time).Round(time.Second)
		if pod.CreationTimestamp.IsZero() {
			age = 0
		}
		if opts.Wide {
			fmt.Fprintf(writer, "%s\t%d/%d\t%s\t%d\t%s\t%s\t%s\n", pod.Name, ready, total, podPhase(pod), restarts, age, pod.Status.PodIP, pod.Spec.NodeName)
		} else {
			fmt.Fprintf(writer, "%s\t%d/%d\t%s\t%d\t%s\n", pod.Name, ready, total, podPhase(pod), restarts, age)
		}
	}
	err = writer.Flush()
	return err
}

func (k *Client) Exec(ctx context.Context, namespace string, pod string, opts ExecOptions) (err error) {
	ctx, op := kubeOperation(ctx, "exec", namespace, pod)
	defer func() { op.End(err) }()
	if len(opts.Command) == 0 {
		return fmt.Errorf("exec command must not be empty")
	}
	restConfig := rest.CopyConfig(k.config)
	restConfig.APIPath = "/api"
	restConfig.GroupVersion = &corev1.SchemeGroupVersion
	restConfig.NegotiatedSerializer = serializer.WithoutConversionCodecFactory{CodecFactory: scheme.Codecs}

	request := k.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(pod).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Command: opts.Command,
			Stdin:   opts.Stdin != nil,
			Stdout:  opts.Stdout != nil,
			Stderr:  opts.Stderr != nil,
			TTY:     opts.TTY,
		}, scheme.ParameterCodec)
	executor, err := remotecommand.NewSPDYExecutor(restConfig, "POST", request.URL())
	if err != nil {
		return err
	}
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  opts.Stdin,
		Stdout: opts.Stdout,
		Stderr: opts.Stderr,
		Tty:    opts.TTY,
	})
	return err
}

func (k *Client) ScaleDeployment(ctx context.Context, namespace string, name string, replicas int32) (err error) {
	ctx, op := kubeOperation(ctx, "scale_deployment", namespace, name)
	defer func() { op.End(err) }()
	deploymentClient := k.clientset.AppsV1().Deployments(namespace)
	deployment, err := deploymentClient.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	copy := deployment.DeepCopy()
	copy.Spec.Replicas = &replicas
	_, err = deploymentClient.Update(ctx, copy, metav1.UpdateOptions{})
	return err
}

func kubeOperation(ctx context.Context, operation string, namespace string, name string) (context.Context, *observability.Operation) {
	return observability.StartOperation(ctx, observability.OperationConfig{
		Name: "kova.kube." + operation,
		SpanAttrs: []attribute.KeyValue{
			attribute.String(observability.AttrOperation, operation),
			observability.StringAttr(observability.AttrNamespace, namespace),
			observability.StringAttr(observability.AttrPod, name),
		},
		MetricAttrs: []attribute.KeyValue{
			attribute.String(observability.AttrOperation, operation),
		},
		Counter:  observability.Instrument{Name: "kova_kube_operations_total", Description: "Kubernetes client operations"},
		Duration: observability.Instrument{Name: "kova_kube_operation_duration_seconds", Description: "Kubernetes client operation duration"},
	})
}

func podContainerCounts(pod corev1.Pod) (ready int, total int, restarts int32) {
	statuses := pod.Status.ContainerStatuses
	if len(statuses) == 0 {
		return 0, len(pod.Spec.Containers), 0
	}
	for _, status := range statuses {
		if status.Ready {
			ready++
		}
		restarts += status.RestartCount
	}
	return ready, len(statuses), restarts
}

func podPhase(pod corev1.Pod) corev1.PodPhase {
	if pod.Status.Phase == "" {
		return corev1.PodPending
	}
	return pod.Status.Phase
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
