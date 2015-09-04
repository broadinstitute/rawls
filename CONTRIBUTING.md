Please follow the process below when making a Pull Request
=========================

- [x] Submitter: Rebase to master. DO NOT SQUASH
- [x] Submitter: Open PR and add the markup shown below to your main PR comment
- [x] Submitter: Make sure Swagger is updated if API changes
- [x] Submitter: Make sure documentation for code is complete
- [x] Submitter: Remove any TODOs that should be TODOne or create a story for further TODOs
- [x] Submitter: Include the JIRA issue number in the PR description
- [x] Submitter: Add some description or comments on the PR explaining the hows/whys if it is not obvious
- [x] Submitter: Assign PR to dvoet
- [x] dvoet: does a once-over of the PR
- [x] dvoet: Anoint a lead reviewer (LR). Assign PR to LR
- [x] LR: Initial review by LR and others - LR then assigns back to submitter for updates

repeat as necessary:
    - Rest of team may comment on PR at will
    - Further commits. DO NOT SQUASH. Reassign to LR
    - Update documentation as needed
    - Further review. Reassign to submitter
    - Re-rebase to master.

- [x] LR signs off. Assign to submitter to finalize PR
- [x] LR: sign off on PR, assign to submitter to finalize PR
- [x] Submitter: Verify all tests go green, including CI tests
- [x] Submitter: Squash commits and merge
- [x] Submitter: Check configuration files in Jenkins in case they need changes
- [x] Submitter: Verify deployment worked on dev server
- [x] Submitter: Double check documentation
- [x] Submitter: Inform other teams of any API changes
- [x] Submitter: Mark JIRA issue as resolved once this checklist is completed

- [x] Rebase to master. DO NOT SQUASH
- [x] Make sure Swagger is updated if API changes
- [x] Make sure documentation for code is complete
- [x] Remove any TODOs that should be TODOne or create a story for further TODOs
- [x] Include the JIRA issue number in the PR description
- [x] Add some description or comments on the PR explaining the hows/whys if it is not obvious
- [x] Assign PR to dvoet
- [x] dvoet does a once-over of the PR
- [x] Anoint a lead reviewer (LR). Assign PR to LR
- [x] Initial review by LR and others - LR then assigns back to submitter for updates


Add the following as a check list to your main PR comment
=========================
```
- [ ] Submitter: Rebase to master. DO NOT SQUASH
- [ ] Submitter: Make sure Swagger is updated if API changes
- [ ] Submitter: Make sure documentation for code is complete
- [ ] Submitter: Remove any TODOs that should be TODOne or create a story for further TODOs
- [ ] Submitter: Include the JIRA issue number in the PR description
- [ ] Submitter: Add some description or comments on the PR explaining the hows/whys if it is not obvious
- [ ] Submitter: Assign PR to dvoet
- [ ] dvoet: does a once-over of the PR
- [ ] dvoet: Anoint a lead reviewer (LR). Assign PR to LR
- [ ] LR: Initial review by LR and others - LR then assigns back to submitter for updates
- [ ] LR: sign off on PR, assign to submitter to finalize PR
- [ ] Submitter: Verify all tests go green, including CI tests
- [ ] Submitter: Squash commits and merge
- [ ] Submitter: Check configuration files in Jenkins in case they need changes
- [ ] Submitter: Verify deployment worked on dev server
- [ ] Submitter: Double check documentation
- [ ] Submitter: Inform other teams of any API changes
- [ ] Submitter: Mark JIRA issue as resolved once this checklist is completed
```
