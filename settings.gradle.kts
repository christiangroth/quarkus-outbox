pluginManagement {
  repositories {
    maven {
      url = uri("https://maven.pkg.github.com/christiangroth/gradle-release-notes-plugin")
      credentials {
        username = providers.gradleProperty("gpr.user").orNull
          ?: System.getenv("GITHUB_ACTOR")
        password = providers.gradleProperty("gpr.token").orNull
          ?: System.getenv("GHCR_PAT")
      }
    }
    mavenCentral()
    gradlePluginPortal()
  }
}

rootProject.name = "quarkus-outbox"

include("api")
include("impl")
