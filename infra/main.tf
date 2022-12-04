provider "aws" {
  region = "us-east-2"
}

resource "aws_redshiftserverless_namespace" "rdapp" {
  namespace_name = "rdapp"
}

resource "aws_redshiftserverless_workgroup" "rdapp" {
  workgroup_name = "rdapp"
  namespace_name = "rdapp"
  depends_on = [
    aws_redshiftserverless_namespace.rdapp
  ]
}
