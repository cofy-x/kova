package httpapi

import (
	"net/http"
	"time"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/runnerexec"

	"github.com/labstack/echo/v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (s *Server) handleCancelBuild(c echo.Context) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	if err := s.authorizeBuild(c.Request().Context(), principalFromContext(c), "delete", build); err != nil {
		return forbidden(c)
	}
	if isTerminalPhase(build.Status.Phase) {
		return c.JSON(http.StatusOK, buildJobFromCR(build, s.cfg))
	}
	base := build.DeepCopy()
	if build.Annotations == nil {
		build.Annotations = map[string]string{}
	}
	if build.Annotations[kovav1.CancellationRequestedAnnotation] == "" {
		build.Annotations[kovav1.CancellationRequestedAnnotation] = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if err := s.client.Patch(c.Request().Context(), build, client.MergeFrom(base)); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	buildCancels.Add(c.Request().Context(), 1)
	return c.JSON(http.StatusAccepted, buildJobFromCR(build, s.cfg))
}

func (s *Server) handleExportBuild(c echo.Context) error {
	return s.handleRunnerAction(c, "export", "application/x-ndjson")
}

func (s *Server) handlePreheatBuild(c echo.Context) error {
	return s.handleRunnerAction(c, "preheat", "application/json")
}

func (s *Server) handleRunnerAction(c echo.Context, action, contentType string) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	if err := s.authorizeBuild(c.Request().Context(), principalFromContext(c), "update", build); err != nil {
		return forbidden(c)
	}
	out, err := s.runner().Post(c.Request().Context(), build, action, c.QueryString())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.Blob(http.StatusOK, contentType, out)
}

func (s *Server) runner() runnerexec.Client {
	return runnerexec.Client{Kube: s.kube, BuildkitAddr: s.cfg.BuildkitAddr}
}
