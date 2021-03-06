package reflect

import java.lang.reflect.{ AccessibleObject => jAccessibleObject }

package object reflect {

  // 在新的方案中，ClassManifests被别名为ClassTags，这是因为我们希望集合中的“toArray”与ClassTags一起工作，
  // 但将其更改为使用ClassTag上下文绑定而不别名ClassManifest将打破所有子类和重写“toArray”的人幸运的是，
  // 别名不会妨碍向后兼容性，因此在这种情况下它是理想的，我希望我们也能对清单和类型标签做同样的事情

  // in the new scheme of things ClassManifests are aliased to ClassTags
  // this is done because we want `toArray` in collections work with ClassTags
  // but changing it to use the ClassTag context bound without aliasing ClassManifest
  // will break everyone who subclasses and overrides `toArray`
  // luckily for us, aliasing doesn't hamper backward compatibility, so it's ideal in this situation
  // I wish we could do the same for Manifests and TypeTags though

  // note, by the way, that we don't touch ClassManifest the object
  // because its Byte, Short and so on factory fields are incompatible with ClassTag's

  /** A `ClassManifest[T]` is an opaque descriptor for type `T`.
   *  It is used by the compiler to preserve information necessary
   *  for instantiating `Arrays` in those cases where the element type
   *  is unknown at compile time.
   *
   *  The type-relation operators make an effort to present a more accurate
   *  picture than can be realized with erased types, but they should not be
   *  relied upon to give correct answers. In particular they are likely to
   *  be wrong when variance is involved or when a subtype has a different
   *  number of type arguments than a supertype.
   */
  @deprecated("Use scala.reflect.ClassTag instead", "2.10.0")
  @annotation.implicitNotFound(msg = "No ClassManifest available for ${T}.")
  type ClassManifest[T]  = scala.reflect.ClassTag[T]

  /** The object `ClassManifest` defines factory methods for manifests.
   *  It is intended for use by the compiler and should not be used in client code.
   */
  @deprecated("Use scala.reflect.ClassTag instead", "2.10.0")
  val ClassManifest = ClassManifestFactory

  /** The object `Manifest` defines factory methods for manifests.
   *  It is intended for use by the compiler and should not be used in client code.
   */
  // TODO undeprecated until Scala reflection becomes non-experimental
  // @deprecated("Use scala.reflect.ClassTag (to capture erasures), scala.reflect.runtime.universe.TypeTag (to capture types) or both instead", "2.10.0")
  val Manifest = ManifestFactory

  def classTag[T](implicit ctag: ClassTag[T]) = ctag

  /** Make a java reflection object accessible, if it is not already
   *  and it is possible to do so. If a SecurityException is thrown in the
   *  attempt, it is caught and discarded.
   */
  def ensureAccessible[T <: jAccessibleObject](m: T): T = {
    if (!m.isAccessible) {
      try m setAccessible true
      catch { case _: SecurityException => } // does nothing
    }
    m
  }

  // anchor for the class tag materialization macro emitted during tag materialization in Implicits.scala
  // implementation is hardwired into `scala.reflect.reify.Taggers`
  // using the mechanism implemented in `scala.tools.reflect.FastTrack`
  // todo. once we have implicit macros for tag generation, we can remove this anchor
  private[scala] def materializeClassTag[T](): ClassTag[T] = macro ???
}

/** An exception that indicates an error during Scala reflection */
case class ScalaReflectionException(msg: String) extends Exception(msg)
