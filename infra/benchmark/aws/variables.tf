variable "aws_region" {
  type = string
}

variable "run_id" {
  type = string
}

variable "topology" {
  type = string
}

variable "client_placement" {
  type = string
}

variable "operator_cidrs" {
  type = list(string)
}

variable "ssh_public_key" {
  type = string
}

variable "ssh_user" {
  type = string
}

variable "coordinator_instance_type" {
  type = string
}

variable "client_instance_type" {
  type = string
}

variable "storage_instance_type" {
  type = string
}

variable "coordinator_volume_gib" {
  type = number
}
