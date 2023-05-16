import Horton from './src';

async function run() {
  const h = new Horton({
    connectionOptions: {
      connectionString: 'postgres://postgres:postgrespw@127.0.0.1:5432',
    },
    tableListeners: {
      test_table: true,
    },
  });

  await h.connect();

  h.on('test_table:INSERT', (...args: unknown[]) => {
    debugger;
  });
}

run()
  // .then(() => process.exit(0))
  .catch(error => {
    console.error(error);

    process.exit(1);
  });
