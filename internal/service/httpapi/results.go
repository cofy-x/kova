package httpapi

import (
	"fmt"
	"net/http"

	kovav1 "github.com/cofy-x/kova/internal/apis/kova/v1alpha1"
	"github.com/cofy-x/kova/internal/service/buildresult"

	"github.com/labstack/echo/v4"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func (s *Server) handleBuildResults(c echo.Context) error {
	build, err := s.getBuild(c.Request().Context(), c.Param("id"))
	if apierrors.IsNotFound(err) {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	results := apiResults(build.Status.Results)
	if len(results) == 0 && isTerminalPhase(build.Status.Phase) {
		build.Status.Results = buildresult.Resolve(c.Request().Context(), s.runner(), build)
		results = apiResults(build.Status.Results)
		build.Status.SourceDigest = build.Spec.SourceDigest
		build.Status.IdempotencyKey = build.Spec.IdempotencyKey
		if err := s.client.Status().Update(c.Request().Context(), build); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("persist build results: %v", err)})
		}
	}
	if len(results) == 0 {
		results = apiResults(buildresult.Pending(build))
	}
	return c.JSON(http.StatusOK, buildResultsResponse{ID: build.Name, SourceDigest: build.Spec.SourceDigest, IdempotencyKey: build.Spec.IdempotencyKey, Results: results})
}

func apiResults(results []kovav1.BuildResult) []BuildResult {
	out := make([]BuildResult, 0, len(results))
	for _, result := range results {
		out = append(out, BuildResult{Format: result.Format, Status: result.Status, Repository: result.Repository, ManifestDigest: result.ManifestDigest, MediaType: result.MediaType, Size: result.Size, Error: result.Error})
	}
	return out
}
