package newspipe.domain.models


import io.circe.Decoder

import io.circe.Encoder

import io.circe.generic.semiauto.deriveDecoder

import io.circe.generic.semiauto.deriveEncoder


enum ValidationStatus:

  case Usable, TemporarilyUnavailable, Invalid, NeedsInvestigation


final case class SourceCandidate(

    publisherId: String,

    title: Option[String],

    provenance: Option[String],

    urls: List[String]

)


final case class ProbeResponse(

    statusCode: Int,

    finalUrl: String,

    contentType: String,

    body: String

)


final case class FeedValidation(

    url: String,

    status: ValidationStatus,

    reason: String,

    checkedAtMillis: Long,

    detail: String,

    feedType: Option[String],

    entryCount: Int,

    hasDates: Boolean,

    hasIds: Boolean

)


final case class CandidateReport(candidate: SourceCandidate, validations: List[FeedValidation])


final case class ValidationSnapshot(

    version: Int,

    generatedAtMillis: Long,

    reports: List[CandidateReport]

)


object SourceValidationModels:

  given Encoder[ValidationStatus] = Encoder.encodeString.contramap(_.toString)

  given Decoder[ValidationStatus] = Decoder.decodeString.emap { value =>

    ValidationStatus.values.find(_.toString == value).toRight("unknown validation status: " + value)

  }

  given Encoder[SourceCandidate] = deriveEncoder

  given Decoder[SourceCandidate] = deriveDecoder

  given Encoder[FeedValidation] = deriveEncoder

  given Decoder[FeedValidation] = deriveDecoder

  given Encoder[CandidateReport] = deriveEncoder

  given Decoder[CandidateReport] = deriveDecoder

  given Encoder[ValidationSnapshot] = deriveEncoder

  given Decoder[ValidationSnapshot] = deriveDecoder