output "region" {
  value = var.aws_region
}

output "primary_az" {
  value = local.azs[0]
}

output "public_client_ip" {
  value = aws_instance.node["client"].public_ip
}

output "private_ips" {
  value = {
    for name, instance in aws_instance.node : name => instance.private_ip
  }
}

output "instance_ids" {
  value = {
    for name, instance in aws_instance.node : name => instance.id
  }
}
