import Horton from './src';

async function run() {
  const h = new Horton({
    connectionOptions: {
      connectionString: 'postgres://postgres:postgrespw@localhost:32776',
    },
  });

  await h.connect();

  const listener = h.createListener('test_table');

  // listener.on('INSERT',
}

run()
  // .then(() => process.exit(0))
  .catch(error => {
    console.error(error);

    process.exit(1);
  });
