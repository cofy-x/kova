package httpapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/queueadmission"
	apiv1 "github.com/cofy-x/kova/pkg/api/v1"

	"github.com/labstack/echo/v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const maxBuildLogResponseBytes = 4 << 20

var errBuildLogsTooLarge = errors.New("requested build log tail exceeds 4 MiB")

// Keep the buffer private so io.WriteString cannot bypass the response cap.
type boundedLogBuffer struct {
	buffer   bytes.Buffer
	overflow bool
}

func (b *boundedLogBuffer) Write(data []byte) (int, error) {
	remaining := maxBuildLogResponseBytes - b.buffer.Len()
	if len(data) > remaining {
		_, _ = b.buffer.Write(data[:remaining])
		b.overflow = true
		return remaining, errBuildLogsTooLarge
	}
	return b.buffer.Write(data)
}

var _ io.Writer = (*boundedLogBuffer)(nil)

func (s *Server) handleListBuilds(c echo.Context) error {
	principal := principalFromContext(c)
	limit, err := strconv.ParseInt(strings.TrimSpace(defaultString(c.QueryParam("limit"), "100")), 10, 64)
	if err != nil || limit < 1 || limit > apiv1.MaxListBuildsPageSize {
		return invalidRequest(c, fmt.Errorf("limit must be between 1 and %d", apiv1.MaxListBuildsPageSize))
	}
	options := []client.ListOption{client.InNamespace(s.cfg.Namespace), client.Limit(limit)}
	if token := strings.TrimSpace(c.QueryParam("continue")); token != "" {
		options = append(options, client.Continue(token))
	}
	if err := s.authorize(c.Request().Context(), principal, "list", ""); err != nil {
		options = append(options, client.MatchingLabels{requesterLabel: requesterID(principal.Username)})
	}
	var builds kovav1.KovaBuildList
	if err := s.reader.List(c.Request().Context(), &builds, options...); err != nil {
		return internalError(c, err)
	}
	jobs := make([]apiv1.BuildJob, 0, len(builds.Items))
	for i := range builds.Items {
		jobs = append(jobs, buildJobFromCR(&builds.Items[i], s.cfg))
	}
	return c.JSON(http.StatusOK, apiv1.JobList{Jobs: jobs, Continue: builds.Continue})
}

func (s *Server) handleGetBuild(c echo.Context) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		intent, found, lookupErr := s.queueStore().Lookup(c.Request().Context(), c.Param("id"))
		if lookupErr != nil {
			return internalError(c, lookupErr)
		}
		principal := principalFromContext(c)
		if found && (intent.RequesterHash == queueadmission.HashRequester(principal.Username) || s.authorize(c.Request().Context(), principal, "get", c.Param("id")) == nil) {
			return queueAdmissionPending(c, c.Param("id"))
		}
		return notFound(c)
	}
	if err != nil {
		return internalError(c, err)
	}
	if err := s.authorizeBuild(c.Request().Context(), principalFromContext(c), "get", build); err != nil {
		return forbidden(c)
	}
	return c.JSON(http.StatusOK, buildJobFromCR(build, s.cfg))
}

func (s *Server) handleBuildLogs(c echo.Context) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		return notFound(c)
	}
	if err != nil {
		return internalError(c, err)
	}
	if err := s.authorizeBuild(c.Request().Context(), principalFromContext(c), "get", build); err != nil {
		return forbidden(c)
	}
	tail, err := strconv.ParseInt(strings.TrimSpace(defaultString(c.QueryParam("tail_lines"), "100")), 10, 64)
	if err != nil || tail < 0 || tail > apiv1.MaxLogTailLines {
		return invalidRequest(c, fmt.Errorf("tail_lines must be between 0 and %d", apiv1.MaxLogTailLines))
	}
	var out boundedLogBuffer
	if build.Status.RunnerPodName == "" {
		return logsUnavailable(c, http.StatusNotFound, "build logs are not available yet", true)
	}
	if isTerminalPhase(build.Status.Phase) {
		return logsUnavailable(c, http.StatusGone, "build logs are only available while the runner is active", false)
	}
	err = s.kube.WritePodLogsTail(c.Request().Context(), build.Namespace, build.Status.RunnerPodName, tail, &out)
	if out.overflow || errors.Is(err, errBuildLogsTooLarge) {
		return logsUnavailable(c, http.StatusRequestEntityTooLarge, "requested build log tail exceeds 4 MiB; request fewer tail lines", false)
	}
	if err != nil {
		return internalError(c, err)
	}
	return c.Blob(http.StatusOK, "text/plain; charset=utf-8", out.buffer.Bytes())
}

func tailLogLines(raw []byte, lines int64) []byte {
	if lines == 0 || len(raw) == 0 {
		return nil
	}
	if lines < 0 {
		return raw
	}
	end := len(raw)
	if raw[end-1] == '\n' {
		end--
	}
	start := end
	for remaining := lines; remaining > 0 && start > 0; {
		start--
		if raw[start] == '\n' {
			remaining--
			if remaining == 0 {
				start++
				break
			}
		}
	}
	return raw[start:]
}

func (s *Server) getBuild(ctx context.Context, id string) (*kovav1.KovaBuild, error) {
	var build kovav1.KovaBuild
	if err := s.reader.Get(ctx, client.ObjectKey{Namespace: s.cfg.Namespace, Name: id}, &build); err != nil {
		return nil, err
	}
	return &build, nil
}
