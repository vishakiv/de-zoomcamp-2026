terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "7.17.0"
    }
  }
}

provider "google" {
  project     = "datatalks-de-course-485418"
  region      = "europe-west3"
  #credentials = file("keys/datatalks-de-course-485418-7badb72593fc.json")
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "datatalks-de-course-485418-terra-bucket"
  location      = "EU"
  force_destroy = true

uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}