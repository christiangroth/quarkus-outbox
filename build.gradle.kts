import de.chrgroth.gradle.plugins.releasenotes.ReleasenotesConfiguration

plugins {
  id("kotlin-project")
  alias(libs.plugins.buildTimeTracker)
  alias(libs.plugins.versionCatalogUpdate)

  alias(libs.plugins.release)
  id("de.chrgroth.gradle.release-notes") version "1.0.1"

  id("dev.iurysouza.modulegraph") version "0.13.0"
}

repositories {
  mavenCentral()
  gradlePluginPortal()
}

kover {
  reports {
    verify {
      rule {
        minBound(40)
      }
    }
  }
}

private val releasenotesBasePath = "docs/releasenotes/"

releasenotes {
  mainBranch = "main"
  skipReleaseNotesOnBranchPrefixes = listOf("main", "dependabot/")

  configure {
    ReleasenotesConfiguration(
      name = "repo-markdown",
      outputPath = "$releasenotesBasePath/RELEASENOTES.md",
      snippetsPath = "$releasenotesBasePath/snippets",
      templatesPath = "$releasenotesBasePath/templates",
      bugfixesHeader = "## Bugfixes / Chore",
      bugfixesFooter = "",
      featuresHeader = "## New Features",
      featuresFooter = "",
      highlightsHeader = "",
      highlightsFooter = "",
      updateNoticesHeader = "## Breaking Changes",
      updateNoticesFooter = "",
      preserveWhitespace = true,
      dateFormat = "yyyy.MM.dd",
    )
  }
}

release {
  git {
    requireBranch = "main"
  }
}

// publish wird nach dem Tag-Push automatisch aufgerufen
tasks.named("afterReleaseBuild") {
  dependsOn(subprojects.map { "${it.path}:publish" })
}
