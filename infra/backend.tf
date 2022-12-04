terraform {
  cloud {
    organization = "rdapp"

    workspaces {
      name = "test"
    }
  }
}
