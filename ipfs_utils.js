import pub from './db';

module.exports = {
  async keepAddingNewMessagesToIpfsIndex() {
    await pub.addNewMessagesToIpfsIndex();
    return new Promise(((resolve) => {
      setTimeout(() => {
        resolve(pub.keepAddingNewMessagesToIpfsIndex());
      }, 10000);
    }));
  },

  async addIndexesToIpfs() {
    await pub.addMessageIndexToIpfs();
    return pub.addIdentityIndexToIpfs();
  },
};
