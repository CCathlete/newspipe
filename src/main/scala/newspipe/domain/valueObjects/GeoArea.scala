package newspipe.domain.valueObjects

opaque type GeoArea = String

object GeoArea:
  def apply(s: String): GeoArea = s
  extension (g: GeoArea) def value: String = g
