pipeline {
  agent {
    node {
      label 'jworkerpapp2'
    }
  }

  stages {
    stage("Deploy Staging") {
      steps{
        withCredentials([sshUserPrivateKey(credentialsId: "${CREDENTIAL_ID}", keyFileVariable: 'KEY_FILE')]) {
            sh '''
                eval `ssh-agent -s` &&
                ssh-add ${KEY_FILE} &&
                ssh-add -L &&
                ssh -o StrictHostKeyChecking=no -v ${REMOTE_USER}@${REMOTE_HOST} "cd ${REMOTE_PATH}/project_dmp && git checkout develop && git pull origin develop" &&
                ssh -o StrictHostKeyChecking=no -v ${REMOTE_USER}@${REMOTE_HOST} "chmod +x ${REMOTE_PATH}/project_dmp/${DEPLOY_SCRIPT_FILE}"
                ssh -o StrictHostKeyChecking=no -v ${REMOTE_USER}@${REMOTE_HOST} "${REMOTE_PATH}/project_dmp/${DEPLOY_SCRIPT_FILE} ${REMOTE_PATH} ${BRANCH_NAME}" &&
                ssh-add -D
            '''
        }
      }
    }
  }
  post {
    success {
      mail to: "${JENKINS_NOTIF_EMAIL}",
          subject: "[JENKINS] SUCCESS: ${currentBuild.projectName}",
          body: "[JENKINS] SUCCESS: ${currentBuild.projectName} ${env.BUILD_URL}"
    }
    failure {
      mail to: "${JENKINS_NOTIF_EMAIL}",
          subject: "[JENKINS] FAILED: ${currentBuild.projectName}",
          body: "[JENKINS] FAILED: ${currentBuild.projectName} ${env.BUILD_URL}"
    }
  }
}
