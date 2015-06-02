package org.broadinstitute.dsde.rawls

import org.broadinstitute.dsde.rawls.model._
import org.broadinstitute.dsde.rawls.workspace.EntityUpdateOperations.{EntityUpdateOperation, RemoveAttribute, AddUpdateAttribute}
import org.joda.time.DateTime

import scala.util.Random

import WorkspaceGenerator._

object WorkspaceGenerator {
  val rand = new Random

  // TODO move these to config?
  val objectNameLen = 10 // length of names for entities / method configs
  val (minKeyLen, maxKeyLen) = (5, 20) // length of property key (for attribute, input, output, prereq, etc)
  val (minStrLen, maxStrLen) = (10, 100) // length of value strings
  val (minValSeqLen, maxValSeqLen) = (1, 100) // length of value lists

  def randLen(min: Int, max: Int) = min + rand.nextInt(max - min)

  def generateBoolean = AttributeBoolean(rand.nextBoolean())
  def generateNumber = AttributeNumber(rand.nextDouble())

  def generateString = AttributeString(rand.alphanumeric.take(randLen(minStrLen, maxStrLen)).mkString)
  def generatePropertyKey = rand.alphanumeric.take(randLen(minKeyLen, maxKeyLen)).mkString
  def generateObjectName = rand.alphanumeric.take(objectNameLen).mkString

  def pickAttributeFrom(choices: String*) = AttributeString(choices(rand.nextInt(choices.length)))

  def generateAnyPrimitive = rand.nextInt(3) match {
    case 0 => generateBoolean
    case 1 => generateNumber
    case 2 => generateString
  }

  def generateValueList = {
    val gen = for (i <- 0 to randLen(minValSeqLen, maxValSeqLen)) yield generateAnyPrimitive
    AttributeValueList(gen.toSeq)
  }

  def generateAnyValue = rand.nextInt(5) match {
    case 0 => generateValueList // 20% chance of list
    case _ => generateAnyPrimitive
  }

  def generateEntityAnnotations(n: Int) = {
    (for (i <- 0 to n) yield (generatePropertyKey, generateAnyValue)).toMap
  }

  def generateMethodConfigValue = rand.nextInt(3) match {
    case 0 => generateBoolean.value.toString
    case 1 => generateNumber.value.toString
    case 2 => generateString.value.toString
  }

  def generateMethodConfigParameters(n: Int) = {
    // this works equally well for inputs, outputs or prerequisites
    (for (i <- 0 to n) yield (generatePropertyKey, generateMethodConfigValue)).toMap
  }

  def generateMethod = {
    // don't really care about method content
    Method("foo", "bar", "baz")
  }
}

class WorkspaceGenerator(workspaceNamespace: String, workspaceName: String) {
  val workspace = Workspace(workspaceNamespace, workspaceName, DateTime.now, "WorkspaceGenerator", Map.empty)
  val wn = WorkspaceName(workspaceNamespace, workspaceName)

  // need to keep track of objects in the workspace, so that we can:
  // (1) make sure that new objects don't collide with existing ones
  // (2) randomly select from the existing objects
  private val entities = scala.collection.mutable.Map[String, scala.collection.mutable.Set[String]]()
  private val methodConfigs = scala.collection.mutable.Set[String]()

  private val singleTypes = List("sample", "individual", "pair")
  val methodConfigNamespace = "allConfigs" // only one namespace for now

  (singleTypes ++ singleTypes.map(getSetType)).foreach(entities += _ -> scala.collection.mutable.Set())

  private def getSetType(entityType: String) = entityType + "_set"

  private def makeEntityName(entityType: String) = {
    var name = generateObjectName
    while (entities(entityType).contains(name)) name = generateObjectName
    entities(entityType) += name
    name
  }

  private def makeMethodConfigName = {
    var name = generateObjectName
    while (methodConfigs.contains(name)) name = generateObjectName
    methodConfigs += name
    name
  }

  private def generateReferenceSingle(entityType: String) = {
    AttributeReferenceSingle(entityType, pickEntity(entityType))
  }

  private def generateReferenceList(entityType: String, n: Int) = {
    AttributeReferenceList(pickEntities(entityType, n).map(AttributeReferenceSingle(entityType, _)))
  }

  def pickEntity(entityType: String) = entities(entityType).toSeq(rand.nextInt(entities(entityType).size))
  def pickMethodConfig = methodConfigs.toSeq(rand.nextInt(methodConfigs.size))

  def pickEntities(entityType: String, n: Int) = rand.shuffle(entities(entityType).toSeq).take(n)
  def pickMethodConfigs(n: Int) = rand.shuffle(methodConfigs.toSeq).take(n)
  def deleteEntities(entityType: String, entityNames: Seq[String]) = entityNames.map(entities(entityType).remove(_))
  def deleteMethodConfigs(configNames: Seq[String]) = configNames.map(methodConfigs.remove(_))

  def createEntitySingle(entityType: String, nRandomAnnotations: Int, specialAnnotations: Map[String, Attribute]) = {
    Entity(makeEntityName(entityType), entityType, generateEntityAnnotations(nRandomAnnotations) ++ specialAnnotations, wn)
  }

  def createEntitySet(memberType: String, nMembers: Int) = {
    Entity(makeEntityName(getSetType(memberType)), getSetType(memberType),
      Map("members" -> generateReferenceList(memberType, nMembers)), wn)
  }

  def createSample(nAnnotations: Int) = {
    createEntitySingle("sample", nAnnotations, Map("type" -> pickAttributeFrom("tumor", "normal", "metastasis")))
  }

  def createPair(nAnnotations: Int) = {
    // TODO check that case and control are distinct samples?
    createEntitySingle("pair", nAnnotations, Map("case" -> generateReferenceSingle("sample"), "control" -> generateReferenceSingle("sample")))
  }

  def createIndividual(nAnnotations: Int, nSamples: Int) = {
    // TODO check that samples only belong to one individual?
    createEntitySingle("individual", nAnnotations, Map("hasSample" -> generateReferenceList("sample", nSamples)))
  }

  /**
   * Create a series of update operations on an entity's annotations.
   * For now, this is just add/update/delete. TODO include add/remove from list
   */
  def updateEntityAnnotations(entity: Entity, nDelete: Int, nModify: Int, nCreate: Int): Seq[EntityUpdateOperation] = {
    val (toDelete, toModify) = rand.shuffle(entity.attributes.filter(_._2.isInstanceOf[AttributeValue]).keys).take(nDelete + nModify).splitAt(nDelete)
    val toCreate = for (i <- 0 to nCreate) yield generatePropertyKey
    val addOrUpdateOps = (toModify ++ toCreate).map(k => AddUpdateAttribute(k, generateAnyValue))
    val removeOps = toDelete.map(k => RemoveAttribute(k))
    (addOrUpdateOps ++ removeOps).toSeq
  }

  def createMethodConfig(nParamsEachType: Int) = {
    // don't care about entity type, namespace, etc
    MethodConfiguration(makeMethodConfigName, "foo", generateMethod,
      generateMethodConfigParameters(nParamsEachType),
      generateMethodConfigParameters(nParamsEachType),
      generateMethodConfigParameters(nParamsEachType),
      wn, methodConfigNamespace)
  }

  def updateParameterSet(params: Map[String, String], nDelete: Int, nModify: Int, nCreate: Int) = {
    val (toDelete, toModify) = rand.shuffle(params.keys).take(nDelete + nModify).splitAt(nDelete)
    val unchanged = params.filterNot((toDelete ++ toModify).toSet)
    val modified = toModify.map(k => (k, generateMethodConfigValue)).toMap
    val created = generateMethodConfigParameters(nCreate)
    unchanged ++ modified ++ created
  }

  /**
   * Generate a new method config from an existing one by deleting, modifying and/or creating parameters.
   * If there aren't enough parameters to go around, delete takes precedence over modify.
   * For now we apply the same operations to prereqs, inputs and outputs, for simplicity.
   */
  def createUpdatedMethodConfig(config: MethodConfiguration, nDelete: Int, nModify: Int, nCreate: Int) = {
    MethodConfiguration(config.name, config.rootEntityType, config.method,
      updateParameterSet(config.prerequisites, nDelete, nModify, nCreate),
      updateParameterSet(config.inputs, nDelete, nModify, nCreate),
      updateParameterSet(config.outputs, nDelete, nModify, nCreate),
      config.workspaceName, config.namespace
    )
  }

  // TODO update references in an entity set
  // TODO rename entity / method config
}
