Add the following to your main PR comment
=========================
```
- [ ] **Submitter**: Rebase to develop. DO NOT SQUASH
- [ ] **Submitter**: Make sure Swagger is updated if API changes
- [ ] **Submitter**: Make sure documentation for code is complete
- [ ] **Submitter**: Review code comments; remove done TODOs, create stories for remaining TODOs
- [ ] **Submitter**: Include the JIRA issue number in the PR description
- [ ] **Submitter**: Add description or comments on the PR explaining the hows/whys (if not obvious)
- [ ] **Submitter**: **Assign PR to** ![](http://i.imgur.com/9dLzbPd.png)
- [ ] ![](http://i.imgur.com/9dLzbPd.png): does a once-over of the PR
- [ ] ![](http://i.imgur.com/9dLzbPd.png): Anoint a lead reviewer (LR). **Assign PR to LR**
- [ ] **LR**: Initial review by LR and others.
- [ ] Comment / review / update cycle:
  * Rest of team may comments on PR at will
  * **LR assigns to submitter** for feedback fixes
  * Submitter updates documentation as needed
  * Submitter rebases to develop again if necessary
  * Submitter makes further commits. DO NOT SQUASH. **Reassign to LR** for further feedback
- [ ] **LR**: sign off, **assign to submitter** to finalize
- [ ] **Submitter**: Squash commits, rebase if necessary
- [ ] **Submitter**: Verify all tests go green, including CI tests
- [ ] **Submitter**: Merge to develop 
- [ ] **Submitter**: Delete branch after merge
- [ ] **Submitter**: Check configuration files in Jenkins in case they need changes
- [ ] **Submitter**: Verify swagger UI on dev server still works after deployment
- [ ] **Submitter**: Inform other teams of any API changes via hipchat and/or email
- [ ] **Submitter**: Mark JIRA issue as resolved once this checklist is completed
```
