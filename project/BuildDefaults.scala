object BuildDefaults {

  def buildScalaVersion = sys.props.getOrElse("scala.version", "2.12.1")
  def buildVersion = "0.1.0"
  def buildOrganization = "uk.co.appministry"
  def akkaVersion = "2.4.17"

}