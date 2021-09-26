package org.nupsea.flink.utils.conf

import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.{GetSecretValueRequest, GetSecretValueResponse}

object MyVarBuilder {

  def readAwsSecret(secretId: String): String = {
    val client = SecretsManagerClient.create()
    val getSecretValueRequest = GetSecretValueRequest.builder().secretId(secretId).build()
    val getSecretValueResult: GetSecretValueResponse = client.getSecretValue(getSecretValueRequest)
    val secret = getSecretValueResult.secretString()
    secret
  }

  def apply(myVar: MyVar): String = {
    myVar.`type`.toLowerCase match {
      case "text" => myVar.value
      case "aws_secret" => readAwsSecret(myVar.value)
      case _ => throw new InvalidConfException(s"Unknown variable type '${myVar.`type`}")
    }
  }
}
