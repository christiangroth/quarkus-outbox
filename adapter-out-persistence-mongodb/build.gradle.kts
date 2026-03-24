plugins {
  id("kotlin-project")
  alias(libs.plugins.allopen)
}

dependencies {
  api(project(":domain-impl"))

  implementation(platform(libs.quarkusBom))
  implementation("io.quarkus:quarkus-micrometer")
  implementation("io.quarkus:quarkus-mongodb-panache-kotlin")
}

allOpen {
  annotation("jakarta.enterprise.context.ApplicationScoped")
  annotation("io.quarkus.mongodb.panache.kotlin.PanacheMongoCompanionBase")
}

// Quarkus Panache's bytecode enhancer inserts instructions into compiled methods but does not
// update LocalVariableTable offsets. Java 25 validates these offsets strictly, causing a
// ClassFormatError if start_pc values become invalid after the transformation.
// Compiling without parameter null-assertion checks ensures LocalVariableTable entries start
// at offset 0, which remains valid after any Panache bytecode insertion.
tasks {
  kotlin {
    compilerOptions.freeCompilerArgs.add("-Xno-param-assertions")
  }
}
