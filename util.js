module.exports = {
  removeDuplicates(arr) {
    const seen = {};
    return arr.filter((item) => {
      if (Object.prototype.hasOwnProperty.call(seen, item)) {
        return false;
      }
      seen[item] = true;
      return true;
    });
  },
  sortByKey(a, b) {
    if (a.key < b.key) {
      return -1;
    }
    if (a.key > b.key) {
      return 1;
    }
    return 0;
  },
  timeoutPromise(promise, timeout) {
    return Promise.race([
      promise,
      new Promise(((resolve) => {
        setTimeout(() => {
          // console.log('promise timed out');
          resolve();
        }, timeout);
      })),
    ]);
  },
};
