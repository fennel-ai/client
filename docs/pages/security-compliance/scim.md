---
title: 'SCIM'
order: 0
status: 'published'
---


# SCIM Provisioning

Fennel supports SCIM to allow external identity providers (like Okta) to
automatically provision/deprovision user accounts in Fennel. Fennel's SCIM 
integration can also be used to assign provisioned users specific Fennel roles.

This is typically used to trigger automatic account & role provisioning (or deprovisioning)
when people join (or leave) your organization and/or appropriate teams.

## Okta SCIM Setup

Fennel leverages Okta groups to manage user provisioning and role assignment.
Each group configured in Okta and assigned to the Fennel App will create a new 
role. Here are the steps for configuring SCIM on Okta:


1. Setup [Fennel SAML app integration on Okta](/security-compliance/sso)
2. Open your Fennel console, visit Settings -> SSO and enable SCIM Provisioning 
   by toggling the "Enable SCIM" button. Keep the following generated fields
   handy for further steps:
    - SCIM endpoint : used as the connector base URL, typically looks like
        `https://your.cluster.fennel.ai/scim`
    - SCIM authentication token : used to configuring SCIM authentication in Okta. 
      Note that this token is highly sensitive, and it's crucial to safeguard it 
      from any potential leaks. 
    ![Diagram](/assets/scim_console.png)

3. Enable SCIM Provisioning on your Okta app with the following settings:
    - SCIM Connector base URL: use SCIM endpoint from the above step
    - Unique identifier field for users: `email`
    - Supported provisioning actions:
        - Push New Users
        - Push Profile Updates
        - Push Groups
    - Select the "HTTP Header" Authentication Mode and copy the SCIM 
      authenetication token from step 2 above into the "Authorization Bearer" field.

    ![Diagram](/assets/scim_okta.png)

And that's it!

Note that once any Fennel role is created by Okta, its specific access levels
can be configured in Fennel Console. And you can always contact Fennel support for 
any issues related to role/user management.