package org.broadinstitute.dsde.rawls.dataaccess

import org.broadinstitute.dsde.rawls.datamigration.MigrationEntry
import org.broadinstitute.dsde.rawls.model._
import org.joda.time.DateTime

import scala.collection.{Map, concurrent}
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe=>ru}

/**
 * This class is a wrapper around scala.reflect.api.Type that caches results of calls to the Type so
 * we don't need to go through reflection for future calls. Caching is done through lazy vals in most
 * cases but TrieMaps are used in places where results build over time.
 *
 * This class has a type parameter so that the compiler can differentiate between the different CachedType instances.
 * The rest of the code does not care about the type thus the AnyCachedType everywhere.
 *
 * @param tpe
 * @tparam T
 */
class CachedType[T](val tpe: Type) {
  def this()(implicit ttag: TypeTag[T]) = this(typeOf[T])

  lazy val ctorProperties: Iterable[(AnyCachedType, String)] = {
    val ctor = tpe.decl(ru.termNames.CONSTRUCTOR).asMethod

    for (paramSymbol <- ctor.paramLists.head) yield {
      (new CachedType(paramSymbol.typeSignature), paramSymbol.name.decodedName.toString.trim)
    }
  }

  val subTypeCache = concurrent.TrieMap[AnyCachedType, Boolean]()
  def <:< (that: AnyCachedType): Boolean = subTypeCache.getOrElseUpdate(that, tpe <:< that.tpe)

  val equivTypeCache = concurrent.TrieMap[AnyCachedType, Boolean]()
  def =:= (that: AnyCachedType): Boolean = subTypeCache.getOrElseUpdate(that, tpe =:= that.tpe)

  lazy val vertexClass = VertexSchema.vertexClassOf(tpe)
  lazy val members = tpe.members.map(new CachedTypeMember(_))
  lazy val typeParams = tpe.asInstanceOf[TypeRefApi].args.map(new CachedType(_))
  lazy val isRawlsEnum = tpe.baseClasses.contains(typeOf[RawlsEnumeration[_]].typeSymbol)
  lazy val ctorMirror = {
    val classT = tpe.typeSymbol.asClass
    val classMirror = ru.runtimeMirror(getClass.getClassLoader).reflectClass(classT)
    val ctor = tpe.decl(ru.termNames.CONSTRUCTOR).asMethod
    classMirror.reflectConstructor(ctor)
  }
  lazy val typeSymbol = tpe.typeSymbol

  val enums = concurrent.TrieMap[String, Any]()

  //Sneaky. The withName() method is defined in a sealed trait, so no way to instance it to call it.
  //Instead, we find a subclass of the enum type, instance that, and call withName() on it.
  def enumWithName[R <: RawlsEnumeration[R]](name: String): R = {
    enums.getOrElseUpdate(name, {
      val m = ru.runtimeMirror(getClass.getClassLoader)
      val anEnumElemClass = this.typeSymbol.asClass.knownDirectSubclasses.head.asClass.toType
      val enumModuleMirror = m.staticModule(anEnumElemClass.typeSymbol.asClass.fullName)
      m.reflectModule(enumModuleMirror).instance.asInstanceOf[RawlsEnumeration[_]].withName(name)
    }).asInstanceOf[R]
  }
}

class CachedTypeMember(val symbol: Symbol) {
  lazy val asTerm = symbol.asTerm
  lazy val typeSignature = new CachedType(symbol.typeSignature)
  lazy val name = symbol.name
}

/**
 * CachedTypes are passed into loadObject and saveObject via implicits.
 * This object contains all the CachedType implicits.
 */
object CachedTypes {
  implicit val anyCachedType = new CachedType[Any]
  implicit val booleanCachedType = new CachedType[Boolean]
  implicit val intCachedType = new CachedType[Int]
  implicit val optionAnyCachedType = new CachedType[Option[Any]]
  implicit val seqWildCachedType = new CachedType[Seq[_]]
  implicit val seqAttributeEntityReferenceCachedType = new CachedType[Seq[AttributeEntityReference]]
  implicit val seqAttributeValueCachedType = new CachedType[Seq[AttributeValue]]
  implicit val setWildCachedType = new CachedType[Set[_]]
  implicit val stringCachedType = new CachedType[String]
  implicit val attributeEntityReferenceListCachedType = new CachedType[AttributeEntityReferenceList]
  implicit val attributeEntityReferenceCachedType = new CachedType[AttributeEntityReference]
  implicit val attributeValueListCachedType = new CachedType[AttributeValueList]
  implicit val attributeValueCachedType = new CachedType[AttributeValue]
  implicit val attributeCachedType = new CachedType[Attribute]
  implicit val domainObjectCachedType = new CachedType[DomainObject]
  implicit val entityCachedType = new CachedType[Entity]
  implicit val methodConfigurationShortCachedType = new CachedType[MethodConfigurationShort]
  implicit val methodConfigurationCachedType = new CachedType[MethodConfiguration]
  implicit val rawlsBillingProjectCachedType = new CachedType[RawlsBillingProject]
  implicit val rawlsEnumerationWildCachedType = new CachedType[RawlsEnumeration[_]]
  implicit val rawlsGroupRefCachedType = new CachedType[RawlsGroupRef]
  implicit val rawlsGroupCachedType = new CachedType[RawlsGroup]
  implicit val rawlsUserRefCachedType = new CachedType[RawlsUserRef]
  implicit val rawlsUserCachedType = new CachedType[RawlsUser]
  implicit val submissionCachedType = new CachedType[Submission]
  implicit val userAuthTypeCachedType = new CachedType[UserAuthType]
  implicit val workspaceNameCachedType = new CachedType[WorkspaceName]
  implicit val workspaceCachedType = new CachedType[Workspace]
  implicit val dateTimeCachedType = new CachedType[DateTime]
  implicit val MapStringWildCachedType = new CachedType[Map[String, _]]
  implicit val mapWildWildCachedType = new CachedType[Map[_, _]]
  implicit val rawlsUserInfoType = new CachedType[RawlsUserInfo]
  implicit val rawlsUserInfoListType = new CachedType[RawlsUserInfoList]
  implicit val pendingBucketDeletionsType = new CachedType[PendingBucketDeletions]
  implicit val migrationEntryType = new CachedType[MigrationEntry]

  /** utility to extract the cachedType implicit */
  def cachedTypeOf[T](implicit cachedType: CachedType[T]) = cachedType
}
