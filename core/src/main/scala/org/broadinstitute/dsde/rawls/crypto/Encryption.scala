package org.broadinstitute.dsde.rawls.crypto

import org.apache.commons.codec.binary.Base64

import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import scala.util.{Failure, Success, Try}

case object Aes256Cbc {

  val encryption = "AES"
  val blockSize = 128
  val keySize = 256
  val paddingMode = "PKCS5Padding"
  val cipherMode = "CBC"

  val ranGen = new SecureRandom()

  final def init(mode: Int, secretKey: Array[Byte], iv: Array[Byte]) = {
    val key = new SecretKeySpec(secretKey, encryption)
    val ivSpec = new IvParameterSpec(iv)
    val cipher = Cipher.getInstance(s"$encryption/$cipherMode/$paddingMode")
    cipher.init(mode, key, ivSpec)
    cipher
  }

  final def validateLength(arrayName: String, array: Array[Byte], expectedBitLength: Int): Try[Unit] =
    if (array.length * 8 == expectedBitLength) {
      Success(())
    } else {
      Failure(
        new IllegalArgumentException(
          s"$arrayName size (${array.length * 8} bits) did not match the required length $expectedBitLength"
        )
      )
    }

  implicit class validationChain(x: Try[Unit]) {
    def validateAnotherLength(arrayName: String, array: Array[Byte], expectedBitLength: Int): Try[Unit] =
      x flatMap { _ => validateLength(arrayName, array, expectedBitLength) }
  }

  final def encrypt(plainText: Array[Byte], secretKey: SecretKey): Try[EncryptedBytes] =
    validateLength("Secret key", secretKey.key, keySize) map { _ =>
      val iv = new Array[Byte](blockSize / 8)
      ranGen.nextBytes(iv)

      val cipher = init(Cipher.ENCRYPT_MODE, secretKey.key, iv)
      EncryptedBytes(cipher.doFinal(plainText), iv)
    }

  final def decrypt(encryptedBytes: EncryptedBytes, secretKey: SecretKey): Try[Array[Byte]] =
    validateLength("Secret key",
                   secretKey.key,
                   keySize
    ) validateAnotherLength ("Initialization vector", encryptedBytes.iv, blockSize) map { _ =>
      val cipher = init(Cipher.DECRYPT_MODE, secretKey.key, encryptedBytes.iv)
      cipher.doFinal(encryptedBytes.cipherText)
    }
}

final case class EncryptedBytes(cipherText: Array[Byte], iv: Array[Byte]) {
  def base64CipherText = Base64.encodeBase64String(cipherText)
  def base64Iv = Base64.encodeBase64String(iv)
}

object EncryptedBytes {
  def apply(base64CipherTextString: String, base64IvString: String): EncryptedBytes =
    EncryptedBytes(Base64.decodeBase64(base64CipherTextString), Base64.decodeBase64(base64IvString))
}

final case class SecretKey(key: Array[Byte])
object SecretKey {
  def apply(base64KeyString: String): SecretKey =
    SecretKey(Base64.decodeBase64(base64KeyString))
}
