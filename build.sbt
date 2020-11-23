name := "Twitter-Emoji-Analysis"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.1"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"
// https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.12"
// https://mvnrepository.com/artifact/commons-io/commons-io
libraryDependencies += "commons-io" % "commons-io" % "2.8.0"



assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith "Inject.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Named.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Provider.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Qualifier.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Scope.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Singleton.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "module-info.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Advice.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "AspectException.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "ConstructorInterceptor.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "ConstructorInvocation.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Interceptor.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Invocation.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Joinpoint.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "MethodInterceptor.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "MethodInvocation.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "ArrayStack.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Buffer.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "BufferUnderflowException.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "FastHashMap$1.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "FastHashMap$CollectionView$CollectionViewIterator.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "FastHashMap$CollectionView.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "FastHashMap$EntrySet.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "FastHashMap$KeySet.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "FastHashMap$Values.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "FastHashMap.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "Log.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "LogConfigurationException.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "LogFactory.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "NoOpLog.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "SimpleLog$1.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "SimpleLog.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "UnusedStubClass.class" => MergeStrategy.first


  case "git.properties"  => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}