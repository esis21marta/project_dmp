triggered = false

pipeline {
  agent {
    node {
      label 'jworkerpapp2'
    }
  }

  stages {
    stage('Build') {
      when { changeset "docker/files/docker_ver.conf" }
      steps {
        script {
          triggered = true
        }
        sh '''
        . docker/files/docker_ver.conf
        /opt/sudocker/bin/docker build -f docker/Dockerfile . -t docker.cicd-jfrog.telkomsel.co.id/kedro:${latest_tag} --quiet
        /opt/sudocker/bin/docker tag docker.cicd-jfrog.telkomsel.co.id/kedro:${latest_tag} docker.cicd-jfrog.telkomsel.co.id/kedro:latest
        '''
      }
    }
    stage('Push') {
      when { changeset "docker/files/docker_ver.conf" }
      steps {
        withCredentials([string(credentialsId: "${CREDENTIAL_ID}", variable: 'DOCKER_PASSWORD')]){
          script {
            triggered = true
          }
          sh '''
          . docker/files/docker_ver.conf
          /opt/sudocker/bin/docker push docker.cicd-jfrog.telkomsel.co.id/kedro:${latest_tag}
          /opt/sudocker/bin/docker push docker.cicd-jfrog.telkomsel.co.id/kedro:latest
          '''
        }

      }
    }
    stage('Clean') {
      when { changeset "docker/files/docker_ver.conf" }
      steps {
        script {
          triggered = true
        }
        sh "/opt/sudocker/bin/docker rmi -f \$(/opt/sudocker/bin/docker images | grep kedro | tr -s ' ' | cut -d ' ' -f 3 | uniq)"
      }
    }
  }
  post {
    success {
      script {
        if (triggered) {
          mail to: "${JENKINS_NOTIF_EMAIL}",
              subject: "[JENKINS] SUCCESS: ${currentBuild.projectName}",
              body: "[JENKINS] SUCCESS: ${currentBuild.projectName} ${env.BUILD_URL}"
        }
      }
    }
    failure {
      script {
          if (triggered){
            mail to: "${JENKINS_NOTIF_EMAIL}",
                subject: "[JENKINS] FAILED: ${currentBuild.projectName}",
                body: "[JENKINS] FAILED: ${currentBuild.projectName} ${env.BUILD_URL}"
          }
      }
    }
  }
}
