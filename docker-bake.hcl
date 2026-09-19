variable "KOVA_IMAGE_REPOSITORY" {
  default = "localhost:5002/kova"
}

variable "KOVA_IMAGE_TAG_SUFFIX" {
  default = "dev"
}

variable "KOVA_VERSION" {
  default = "dev"
}

variable "KOVA_COMMIT" {
  default = "unknown"
}

variable "KOVA_BUILD_DATE" {
  default = "unknown"
}

group "images" {
  targets = ["controller", "runner", "worker"]
}

target "_role" {
  context    = "."
  dockerfile = "docker/Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64"]
  args = {
    VERSION    = KOVA_VERSION
    COMMIT     = KOVA_COMMIT
    BUILD_DATE = KOVA_BUILD_DATE
  }
}

target "controller" {
  inherits = ["_role"]
  target   = "controller"
  tags     = ["${KOVA_IMAGE_REPOSITORY}:controller-${KOVA_IMAGE_TAG_SUFFIX}"]
}

target "runner" {
  inherits = ["_role"]
  target   = "runner"
  tags     = ["${KOVA_IMAGE_REPOSITORY}:runner-${KOVA_IMAGE_TAG_SUFFIX}"]
}

target "worker" {
  inherits = ["_role"]
  target   = "worker"
  tags     = ["${KOVA_IMAGE_REPOSITORY}:worker-${KOVA_IMAGE_TAG_SUFFIX}"]
}
