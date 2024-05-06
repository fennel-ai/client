---
title: 'SAML SSO'
order: 0
status: 'published'
---

# SSO With SAML

Fennel, by default, allows using SSO authentication through Google. For SAML based authentication, additional configuration is required. 

This document will walk through setting up SAML based authentication between Fennel (SP) and Okta (IdP).

Okta documentation for configuing the SAML integration is available at https://help.okta.com/en-us/content/topics/apps/apps_app_integration_wizard_saml.htm


## Okta Setup
- Navigate to your Okta admin dashboard
- Choose “Create App Integration”
    - Choose “SAML 2.0” for “Sign-in Method”
    - Choose “Web Application” for “Application type”

- General Settings
    - Name this application - Fennel AI
    - Upload the Fennel logo (download [here](https://avatars.githubusercontent.com/u/94271393?s=280&v=4)).
- Configure SAML
    - Single sign on URL
        - On your Fennel Console, go to Settings -> SSO
        - Copy the ACS URL that has been generated and paste as the SSO url in Okta
        ![Diagram](/assets/sso_console_acs.png)
    - Make sure that “Use this for the Recipient URL and Destination URL” is checked
    - Audience URI - Use your cluster domain. For example `https://test.aws.fennel.ai/`
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
- Feedback
    - Check “I’m an Okta customer adding an internal app”


## Console  Setup
On the resulting page, click “View SAML Setup Instructions”. You can also go to your Okta app and select the `Sign On` tab. You’ll be presented with text boxes showing:

- Identity Provider Single Sign-On URL
- Identity Provider Issuer
- X.509 Certificate

![Diagram](/assets/sso_okta.png)

Go to your Console, go to Settings -> SSO, and input these values. Click Save. 
![Diagram](/assets/sso_console_filled.png)

Your SAML based SSO login is now successfully setup
