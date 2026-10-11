package newspipe.domain.valueObjects

opaque type Urgency = String

object Urgency:
  def apply(s: String): Urgency = s
  extension (u: Urgency) def value: String = u
