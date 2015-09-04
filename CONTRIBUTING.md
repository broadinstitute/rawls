Copy and paste the following into your main PR comment
=========================

- [ ] Rebase to master. DO NOT SQUASH
- [ ] Make sure Swagger is updated if API changes
- [ ] Make sure documentation for code is complete
- [ ] Remove any TODOs that should be TODOne or create a story for further TODOs
- [ ] Include the JIRA issue number in the PR description
- [ ] Add some description or comments on the PR explaining the hows/whys if it is not obvious
- [ ] Assign PR to dvoet
- [ ] dvoet does a once-over of the PR
- [ ] Anoint a lead reviewer (LR). Assign PR to LR
- [ ] Initial review by LR and others - LR then assigns back to submitter for updates
  repeat as necessary:
=========================
  - Rest of team may comment on PR at will
  - Further commits. DO NOT SQUASH. Reassign to LR
  - Update documentation as needed
  - Further review. Reassign to submitter
  - Re-rebase to master.
  - [ ] LR signs off. Assign to submitter to finalize PR
Submitter should then:
=========================
- [ ] Verify all tests go green, including CI tests
- [ ] Squash commits and merge
- [ ] Checks configuration files in Jenkins in case they need changes
- [ ] Verify deployment worked on dev server
- [ ] Double check documentation
- [ ] Inform other teams of any API changes
- [ ] JIRA issue marked as resolved once this checklist is completed



