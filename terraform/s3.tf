terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.91.0"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "us-west-2"
}

resource "aws_s3_bucket" "datalake" {
  bucket = "openweather-datalake-gsc"

  tags = {
    Name = "OpenWeather Datalake"
  }
}