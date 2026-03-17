plugins {
  id("kotlin-project")
  alias(libs.plugins.allopen)
}

dependencies {
  api(project(":domain-api"))

  implementation(platform(libs.quarkusBom))
}

allOpen {
  annotation("jakarta.enterprise.context.ApplicationScoped")
}
