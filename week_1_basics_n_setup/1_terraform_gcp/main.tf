terraform {
  required_providers {
    google = {
        source = "hashicorp/google"
        version = "5.6.0"
    }
  }
}

provider "google" {
    project = "terraform-demo-449207"
    region = "australia-southeast1"
}


resource "google_storage_bucket" "demo-bucket" {
  name          = "terraform-demo-449207-terra-bucket"
  location      = "AU"
  force_destroy = true
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}