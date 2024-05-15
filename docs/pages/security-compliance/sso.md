---
title: 'SSO'
order: 0
status: 'published'
---

# Single-Sign On

Fennel supports enterprise-grade SSO authentication via multiple identity 
providers. Google SSO Authentication works out of the box without any setup. 
For using Okta's SAML based authentication, additional configuration is required
as described below.


## Okta SAML SSO Setup

You can find the standard Okta documentation for configuring the SAML integration 
[here](https://help.okta.com/en-us/content/topics/apps/apps_app_integration_wizard_saml.htm).
Note that for the purpose of this configuration, Fennel is considered a "Service 
Provider" (aka SP) and Okta is considered an "Identity Provider" (aka IdP).

Here are the actual steps:

1. First, open your Fennel console, go to Settings -> SSO and copy the generated 
   ACS URL. Keep it handy - we will be entering this in Okta's dashboard soon.
    ![Diagram](/assets/sso_console_acs.png)
2. Then, navigate to your Okta admin dashboard
3. Choose “Create App Integration”
    - Choose “SAML 2.0” for “Sign-in Method”
    - Choose “Web Application” for “Application type”
4. Configure general settings
    - Name this application (e.g. "Fennel" or any other name of your choice)
    - Upload the Fennel logo (download [here](https://avatars.githubusercontent.com/u/94271393?s=280&v=4))
5. Configure SAML
    - Single sign on URL - paste the ACS URL from step 1 above.
    - Make sure that “Use this for the Recipient URL and Destination URL” is checked
    - Audience URI - use the URL of your Fennel cluster. This typically looks like `https://your.cluster.fennel.ai/`
    - Name ID format - `EmailAddress`
    - Application Username - `Email`
    - Update application username on - `Create and update`
    - Attribute Statements
        - firstName
            - Name format: BASIC
            - Value: `user.firstName`
        - lastName
            - Name format: BASIC
            - Value: `user.lastName`
        - email
            - Name format: BASIC
            - Value: `user.email`
6. Feedback
    - Check “I’m an Okta customer adding an internal app”
7. On the resulting page, click “View SAML Setup Instructions”. You can also go 
   to your Okta app and select the `Sign On` tab. Keep the value of the following
   three text boxes handy for further steps:
    - Sign-On URL
    - Issuer
    - Signing Certificate (aka X.509 Certificate)
    ![Diagram](/assets/sso_okta.png)

8. Open Fennel console again, go to Settings -> SSO, and enter/save the values
   copied in the previous step.
    ![Diagram](/assets/sso_console_filled.png)


Your Okta SAML SSO should now be successfully setup. Contact Fennel Support if
you run into any issues.