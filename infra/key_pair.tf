resource "aws_key_pair" "udacity_dend_4_key" {
  key_name   = "udacity-dend-4-key"
  public_key = file("${path.module}/id_rsa.pub")
}