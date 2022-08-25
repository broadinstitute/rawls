Ticket: <Link to Jira ticket>
<Put notes here to help reviewer understand this PR>

---

**PR checklist**

- [ ] Include the JIRA issue number in the PR description and title
- [ ] Make sure Swagger is updated if API changes
  - [ ] **...and Orchestration's Swagger too!**
- [ ] If you changed anything in `model/`, then you should [publish a new official `rawls-model`](https://github.com/broadinstitute/rawls/blob/develop/README.md) and update `rawls-model` in [Orchestration's dependencies](https://github.com/broadinstitute/firecloud-orchestration/blob/develop/project/Dependencies.scala).
- [ ] Get two thumbsworth of PR review
- [ ] Verify all tests go green, including CI tests
- [ ] **Squash commits and merge** to develop
- [ ] Delete branch after merge
- [ ] Inform other teams of any substantial changes via Slack and/or email
