pipeline {
  environment {
    registry = 'ghcr.io/datakaveri/iudx-data-transformer'
    registryUri = 'https://ghcr.io'
    registryCredential = 'datakaveri-ghcr'
    GIT_HASH = GIT_COMMIT.take(7)
  }

  agent {
    node {
      label 'slave1'
    }
  }

  stages {

    stage('Build Image') {
      steps {
        script {
          echo 'Building image for branch: ' + env.GIT_BRANCH
          image = docker.build(registry, "-f Dockerfile .")
        }
      }
    }

    stage('Push Image') {
      steps {
        script {
          docker.withRegistry(registryUri, registryCredential) {
            image.push("v1.1.0-${env.GIT_HASH}")
          }
        }
      }
    }

  }

  post {
    failure {
      script {
        emailext recipientProviders: [buildUser(), developers()],
        to: '$DEFAULT_RECIPIENTS',
        subject: '$PROJECT_NAME - Build # $BUILD_NUMBER - $BUILD_STATUS!',
        body: '''$PROJECT_NAME - Build # $BUILD_NUMBER - $BUILD_STATUS:
Check console output at $BUILD_URL to view the results.'''
      }
    }
  }
}