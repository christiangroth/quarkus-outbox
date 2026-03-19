plugins {
  id("kotlin-project")
  alias(libs.plugins.allopen)
}

dependencies {
  api(project(":domain-impl"))

  implementation(platform(libs.quarkusBom))
}

allOpen {
  annotation("jakarta.enterprise.context.ApplicationScoped")
}
