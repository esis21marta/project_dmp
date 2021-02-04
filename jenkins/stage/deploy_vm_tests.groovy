pipeline {
  agent {
    node {
      label 'jworkerpapp2'
    }
  }

  stages {
    stage("Unit Test") {
      steps{
        withCredentials([sshUserPrivateKey(credentialsId: "${CREDENTIAL_ID}", keyFileVariable: 'KEY_FILE')]) {
            sh '''
                eval `ssh-agent -s` &&
                ssh-add ${KEY_FILE} &&
                ssh-add -L &&
                ssh -o StrictHostKeyChecking=no -v ${REMOTE_USER}@${REMOTE_HOST} "cd ${REMOTE_PATH}/project_dmp && git checkout develop && git pull origin develop" &&
                ssh -o StrictHostKeyChecking=no -v ${REMOTE_USER}@${REMOTE_HOST} "chmod +x ${REMOTE_PATH}/project_dmp/${UNIT_TEST_SCRIPT_FILE}"
                ssh -o StrictHostKeyChecking=no -v ${REMOTE_USER}@${REMOTE_HOST} "${REMOTE_PATH}/project_dmp/${UNIT_TEST_SCRIPT_FILE}" &&
                ssh-add -D
            '''
        }
      }
    }
    stage("Integration Test") {
      steps{
        withCredentials([sshUserPrivateKey(credentialsId: "${CREDENTIAL_ID}", keyFileVariable: 'KEY_FILE')]) {
            sh '''
                eval `ssh-agent -s` &&
                ssh-add ${KEY_FILE} &&
                ssh-add -L &&
                ssh -o StrictHostKeyChecking=no -v ${REMOTE_USER}@${REMOTE_HOST} "chmod +x ${REMOTE_PATH}/project_dmp/${INTEGRATION_TEST_SCRIPT_FILE}"
                ssh -o StrictHostKeyChecking=no -v ${REMOTE_USER}@${REMOTE_HOST} "${REMOTE_PATH}/project_dmp/${INTEGRATION_TEST_SCRIPT_FILE}" &&
                ssh-add -D
            '''
        }
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
