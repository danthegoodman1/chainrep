data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_ami" "al2023_arm64" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023*-kernel-*-arm64"]
  }
}

locals {
  multi_az = var.topology == "multi-az"
  azs      = slice(data.aws_availability_zones.available.names, 0, local.multi_az ? 3 : 1)

  roles = {
    client = {
      instance_type = var.client_instance_type
      role          = "client"
      subnet_index  = local.multi_az && var.client_placement == "remote-az" ? 1 : 0
      public_ip     = true
      root_volume   = 50
    }
    coordinator = {
      instance_type = var.coordinator_instance_type
      role          = "coordinator"
      subnet_index  = 0
      public_ip     = false
      root_volume   = var.coordinator_volume_gib
    }
    storage-a = {
      instance_type = var.storage_instance_type
      role          = "storage"
      subnet_index  = 0
      public_ip     = false
      root_volume   = 80
    }
    storage-b = {
      instance_type = var.storage_instance_type
      role          = "storage"
      subnet_index  = local.multi_az ? 1 : 0
      public_ip     = false
      root_volume   = 80
    }
    storage-c = {
      instance_type = var.storage_instance_type
      role          = "storage"
      subnet_index  = local.multi_az ? 2 : 0
      public_ip     = false
      root_volume   = 80
    }
  }
}

resource "aws_vpc" "bench" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
}

resource "aws_internet_gateway" "bench" {
  vpc_id = aws_vpc.bench.id
}

resource "aws_subnet" "bench" {
  count                   = length(local.azs)
  vpc_id                  = aws_vpc.bench.id
  availability_zone       = local.azs[count.index]
  cidr_block              = cidrsubnet(aws_vpc.bench.cidr_block, 8, count.index)
  map_public_ip_on_launch = false
}

resource "aws_route_table" "bench" {
  vpc_id = aws_vpc.bench.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bench.id
  }
}

resource "aws_route_table_association" "bench" {
  count          = length(aws_subnet.bench)
  subnet_id      = aws_subnet.bench[count.index].id
  route_table_id = aws_route_table.bench.id
}

resource "aws_security_group" "bench" {
  name        = "chainrep-bench-${var.run_id}"
  description = "chainrep benchmark security group"
  vpc_id      = aws_vpc.bench.id

  ingress {
    description = "operator ssh"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.operator_cidrs
  }

  ingress {
    description = "cluster internal"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_key_pair" "bench" {
  key_name   = "chainrep-bench-${var.run_id}"
  public_key = var.ssh_public_key
}

resource "aws_placement_group" "bench" {
  count    = local.multi_az ? 0 : 1
  name     = "chainrep-bench-${var.run_id}"
  strategy = "cluster"
}

resource "aws_instance" "node" {
  for_each = local.roles

  ami                         = data.aws_ami.al2023_arm64.id
  instance_type               = each.value.instance_type
  subnet_id                   = aws_subnet.bench[each.value.subnet_index].id
  vpc_security_group_ids      = [aws_security_group.bench.id]
  associate_public_ip_address = each.value.public_ip
  key_name                    = aws_key_pair.bench.key_name
  placement_group             = local.multi_az ? null : aws_placement_group.bench[0].name
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    role     = each.value.role
    ssh_user = var.ssh_user
  })

  root_block_device {
    volume_size           = each.value.root_volume
    volume_type           = "gp3"
    delete_on_termination = true
  }

  tags = {
    Name = "chainrep-bench-${var.run_id}-${each.key}"
    role = each.key
  }
}
