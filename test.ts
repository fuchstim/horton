import Horton from './src';

async function run() {
  const h = new Horton({
    connectionOptions: {
      connectionString: 'postgres://postgres:postgrespw@localhost:32776',
    },
    listeners: {
      test_table: true,
    },
  });

  await h.connect();

  // h.on('a:INSERT', () => {

  // });
}

run()
  // .then(() => process.exit(0))
  .catch(error => {
    console.error(error);

    process.exit(1);
  });
