package httpapi

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"

	"github.com/labstack/echo/v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (s *Server) handleListBuilds(c echo.Context) error {
	principal := principalFromContext(c)
	limit, err := strconv.ParseInt(strings.TrimSpace(defaultString(c.QueryParam("limit"), "100")), 10, 64)
	if err != nil || limit < 1 || limit > 500 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "limit must be between 1 and 500"})
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
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	jobs := make([]BuildJob, 0, len(builds.Items))
	for i := range builds.Items {
		jobs = append(jobs, buildJobFromCR(&builds.Items[i], s.cfg))
	}
	return c.JSON(http.StatusOK, jobListResponse{Jobs: jobs, Continue: builds.Continue})
}

func (s *Server) handleGetBuild(c echo.Context) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	if err := s.authorizeBuild(c.Request().Context(), principalFromContext(c), "get", build); err != nil {
		return forbidden(c)
	}
	return c.JSON(http.StatusOK, buildJobFromCR(build, s.cfg))
}

func (s *Server) handleBuildLogs(c echo.Context) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	if err := s.authorizeBuild(c.Request().Context(), principalFromContext(c), "get", build); err != nil {
		return forbidden(c)
	}
	tail, err := strconv.ParseInt(strings.TrimSpace(defaultString(c.QueryParam("tail_lines"), "100")), 10, 64)
	if err != nil || tail < 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "tail_lines must be a non-negative integer"})
	}
	if build.Status.LogArtifactURI != "" {
		reader, err := s.store.Open(c.Request().Context(), build.Status.LogArtifactURI)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("read persisted logs: %v", err)})
		}
		raw, readErr := io.ReadAll(io.LimitReader(reader, s.cfg.MaxLogBytes+1))
		closeErr := reader.Close()
		if readErr != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("read persisted logs: %v", readErr)})
		}
		if closeErr != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("close persisted logs: %v", closeErr)})
		}
		if int64(len(raw)) > s.cfg.MaxLogBytes {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "persisted logs exceed the configured limit"})
		}
		return c.Blob(http.StatusOK, "text/plain; charset=utf-8", tailLogLines(raw, tail))
	}
	var out bytes.Buffer
	if build.Status.RunnerPodName == "" {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job logs are not available"})
	}
	if err := s.kube.WritePodLogsTail(c.Request().Context(), build.Namespace, build.Status.RunnerPodName, tail, &out); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.Blob(http.StatusOK, "text/plain; charset=utf-8", out.Bytes())
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
