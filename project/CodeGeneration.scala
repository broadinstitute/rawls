import com.simplytyped.Antlr4Plugin
import com.simplytyped.Antlr4Plugin.autoImport._
import sbt.Keys._
import sbt._

object CodeGeneration {
  lazy val antlr4CodeGenerationSettings: Seq[Setting[_]] = List(
    /*
     Not sure how/why Antlr4Plugin.projectSettings are not being loaded automatically, so instead we will just
     explicitly load them here...

     If this doesn't look right to you, or a new version of the sbt-antlr4 fixes this, please update/remove this
     explicit loading of the projectSettings, thanks!
     */
    Antlr4Plugin.projectSettings: _*
  ) ++ List(
    /*
    The sbt plugin doesn't behave like the maven plugin, so we have to specify the package...
    https://github.com/ihji/sbt-antlr4/issues/3
     */
    antlr4PackageName in Antlr4 := Option("org.broadinstitute.dsde.rawls.expressions.parser.antlr"),
    antlr4Version in Antlr4 := "4.8-1",
    antlr4GenVisitor in Antlr4 := true,
    antlr4GenListener in Antlr4 := false,
    antlr4TreatWarningsAsErrors in Antlr4 := true,
    /*
    Put the generated code in a sibling to `main`. Otherwise the default, a nested directory `main/antlr4`, trips up
    IntelliJ. It will try to compile the generated source code twice, once under `main`, and again under `main/antlr4`,
    resulting in cryptic errors like "ExtendedJSONBaseVisitor is already defined as class ExtendedJSONBaseVisitor"
     */
    javaSource in Antlr4 := new File((sourceManaged in Compile).value + "_antlr4")
  )
}
