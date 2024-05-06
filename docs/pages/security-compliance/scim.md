---
title: 'SCIM'
order: 0
status: 'draft'
---


# SCIM Provisioning

Fennel's SCIM integration automates adding/removing Users from your Organization and granting access to Fennel Roles by leveraging Okta groups. Each group configured in Okta and assigned to the Fennel App, will create a new role and that you can manage with the Console Interface.

## SCIM Setup

- Setup [Fennel SAML app integration on Okta]("/security-compliance/sso")
- Enable SCIM Provisioning on your Fennel Console by toggling the "Enable SCIM" button under your SSO configuration in the Settings Page. You will see the following generated:
    - SCIM endpoint : Will be used as the connector base URL
    - SCIM authentication token : This token is highly sensitive, and it's crucial to safeguard it from any potential leaks. It will be utilized for configuring Okta's SCIM authentication.
    
    ![Diagram](/assets/scim_console.png)
- Enable SCIM Provisioning on your Okta app with the following settings:
    - SCIM Connector base URL: SCIM URL generated in the above step (Example: `https://test.aws.fennel.ai/scim/`)
    - Unique identifier field for users: `email`
    - Supported provisioning actions:
        - Push New Users
        - Push Profile Updates
        - Push Groups
    - Select the "HTTP Header" Authentication Mode and copy the token from Fennel Console into the Authorization Bearer field.

    ![Diagram](/assets/scim_okta.png)

For any specific role/user management requests, contact the Fennel support team.
