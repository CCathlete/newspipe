package notebookCode

import scala.util.{Success, Try}

object Functors {

  val anIncrementedList = List(1, 2, 3).map(_ + 1)
  val anOption: Option[Int] = Some(2)
  val aTry: Try[Int] = Success(42)

  val aTransformedOption: Option[Int] = anOption.map(_ * 10)
  def do10xOption(opt: Option[Int]): Option[Int] = opt.map(_ * 10)

  val aTransformedList: List[Int] = anIncrementedList.map(_ * 10)
  def do10xList(list: List[Int]): List[Int] = list.map(_ * 10)

  val aTransformedTry: Try[Int] = aTry.map(_ * 10)
  def do10xTry(t: Try[Int]): Try[Int] = t.map(_ * 10)

  // Defining a functor to make the 10x generic.
  // Difference between C[_] and C[Any]? Any is a specific type, C[Any] must
  // always accept any while C[_] can be C[Int], C[String], etc.

  // This trait is a functor factory, the instances of it are the actual
  // functors.
  trait Functor[C[_]] {
    def map[A, B](container: C[A])(f: A => B): C[B]
  }

  object Functor {
    def apply[C[_]](implicit evidence: Functor[C]): Functor[C] = evidence

    implicit val optionFunctor: Functor[Option] = new Functor[Option] {
      def map[A, B](container: Option[A])(f: A => B): Option[B] =
        container.map(f)
    }

    implicit val listFunctor: Functor[List] = new Functor[List] {
      def map[A, B](container: List[A])(f: A => B): List[B] =
        container.map(f)
    }

    implicit val tryFunctor: Functor[Try] = new Functor[Try] {
      def map[A, B](container: Try[A])(f: A => B): Try[B] =
        container.map(f)
    }

    // This would take the instance of the functor of the required type and
    // apply its map method.
    implicit class FunctorSyntax[C[_], A](container: C[A])(implicit
        evidence: Functor[C]
    ) {
      def map[B](f: A => B): C[B] = evidence.map(container)(f)
    }

  }

}
