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
                ssh -o StrictHostKeyChecking=no -v ${REMOTE_USER}@${REMOTE_HOST} "cd ${REMOTE_PATH}/project_dmp && git checkout master && git pull origin master" &&
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
        mail to: 'e7c83c72.365tsel.onmicrosoft.com@apac.teams.ms',
            subject: "Succeeded Pipeline: ${currentBuild.fullDisplayName}",
            body: "${env.BUILD_URL}"
    }
    failure {
        mail to: 'e7c83c72.365tsel.onmicrosoft.com@apac.teams.ms',
            subject: "Failed Pipeline: ${currentBuild.fullDisplayName}",
            body: "Something is wrong with ${env.BUILD_URL}"
    }
  }
}
