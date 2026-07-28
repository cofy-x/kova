package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
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
	if err := s.authorizeBuild(c.Request().Context(), principalFromContext(c), "get", build); err != nil {
		return forbidden(c)
	}
	storedResults := build.Status.Results
	if build.Status.ResultArtifactURI != "" {
		reader, openErr := s.store.Open(c.Request().Context(), build.Status.ResultArtifactURI)
		if openErr != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("read build results: %v", openErr)})
		}
		raw, readErr := io.ReadAll(reader)
		closeErr := reader.Close()
		if readErr != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("read build results: %v", readErr)})
		}
		if closeErr != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("close build results: %v", closeErr)})
		}
		if err := json.Unmarshal(raw, &storedResults); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": fmt.Sprintf("decode build results: %v", err)})
		}
	}
	results := apiResults(storedResults)
	if len(results) == 0 {
		fallback := buildresult.Pending(build)
		for i := range fallback {
			switch build.Status.Phase {
			case kovav1.PhaseFailed:
				fallback[i].Status = "failed"
				fallback[i].Error = build.Status.Message
			case kovav1.PhaseCancelled:
				fallback[i].Status = "cancelled"
			case kovav1.PhaseSucceeded:
				fallback[i].Status = "failed"
				fallback[i].Error = "persisted result is unavailable"
			}
		}
		results = apiResults(fallback)
	}
	return c.JSON(http.StatusOK, buildResultsResponse{
		ID: build.Name, SourceDigest: build.Spec.Source.Digest,
		IdempotencyKey:    build.Spec.IdempotencyKey,
		ResultArtifactURI: build.Status.ResultArtifactURI, Results: results,
	})
}

func apiResults(results []kovav1.BuildResult) []BuildResult {
	out := make([]BuildResult, 0, len(results))
	for _, result := range results {
		out = append(out, BuildResult{Format: result.Format, Status: result.Status, Repository: result.Repository, ManifestDigest: result.ManifestDigest, MediaType: result.MediaType, Size: result.Size, Error: result.Error})
	}
	return out
}
