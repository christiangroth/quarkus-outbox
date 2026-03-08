plugins {
  id("kotlin-project")
  alias(libs.plugins.allopen)
}

dependencies {
  api(project(":domain-api"))

  implementation(platform(libs.quarkusBom))
  implementation("io.quarkus:quarkus-micrometer")
  implementation("io.quarkus:quarkus-scheduler")
}

allOpen {
  annotation("jakarta.enterprise.context.ApplicationScoped")
}
