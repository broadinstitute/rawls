Add the following to your main PR comment
=========================
```
- [ ] **Submitter**: Rebase to master. DO NOT SQUASH
- [ ] **Submitter**: Make sure Swagger is updated if API changes
- [ ] **Submitter**: Make sure documentation for code is complete
- [ ] **Submitter**: Review code comments; remove done TODOs, create stories for remaining TODOs
- [ ] **Submitter**: Include the JIRA issue number in the PR description
- [ ] **Submitter**: Add description or comments on the PR explaining the hows/whys (if not obvious)
- [ ] **Submitter**: **Assign PR to Tech Lead**
- [ ] **Tech Lead**: does a once-over of the PR
- [ ] **Tech Lead**: Anoint a lead reviewer (LR). **Assign PR to LR**
- [ ] **LR**: Initial review by LR and others - **LR assigns back to submitter** for updates
- repeat as necessary:
  * Rest of team may comment on PR at will
  * Further commits. DO NOT SQUASH. **Reassign to LR**
  * Update documentation as needed
  * **LR assigns to submitter**
  * Re-rebase to master.
- [ ] **LR**: sign off on PR, assign to submitter to finalize PR
- [ ] **Submitter**: Verify all tests go green, including CI tests
- [ ] **Submitter**: Squash commits and merge
- [ ] **Submitter**: Check configuration files in Jenkins in case they need changes
- [ ] **Submitter**: Verify swagger UI on dev server still works after deployment
- [ ] **Submitter**: Inform other teams of any API changes via hipchat and/or email
- [ ] **Submitter**: Mark JIRA issue as resolved once this checklist is completed
```