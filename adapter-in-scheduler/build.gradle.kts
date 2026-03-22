plugins {
  id("kotlin-project")
  alias(libs.plugins.allopen)
}

dependencies {
  api(project(":domain-impl"))

  implementation(platform(libs.quarkusBom))
  implementation("io.quarkus:quarkus-scheduler")
  implementation("io.quarkus:quarkus-micrometer")
}

allOpen {
  annotation("jakarta.enterprise.context.ApplicationScoped")
}
