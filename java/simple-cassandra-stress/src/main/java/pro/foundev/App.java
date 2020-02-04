package pro.foundev;

import java.util.Locale;
import java.util.ArrayList;
import java.util.List;
import java.util.function.*;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.net.InetSocketAddress;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.*;
import com.github.javafaker.Faker;

public class App 
{
    public static void main( String[] args )
    {
        List<String> hosts = new ArrayList<>();
        hosts.add("127.0.0.1");
        if (args.length > 0){
            hosts = new ArrayList<>();
            String rawHosts = args[0];
            for (String host: rawHosts.split(",")){
                hosts.add(host.trim());
            }
        }
        int port = 9042;
        if (args.length > 1){
            port = Integer.valueOf(args[1]);
        }
        long trans = 1000000;
        if (args.length > 2){
            trans = Long.valueOf(args[2]);
        }
        int inFlight = 25;
        if (args.length > 3){
            inFlight = Integer.valueOf(args[3]);
        }
        int maxErrors = -1;
        if (args.length > 4){
            maxErrors = Integer.valueOf(args[4]);
        }
        CqlSessionBuilder builder = CqlSession.builder();
        for (String host: hosts){
            builder = builder.addContactPoint(new InetSocketAddress(host, port));
        }
        builder = builder.withLocalDatacenter("datacenter1");
        try(final CqlSession session = builder.build()){
            //setup tables
            session.execute("CREATE KEYSPACE IF NOT EXISTS my_key WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE IF NOT EXISTS my_key.my_table (id uuid, name text, address text, state text, zip text, balance int, PRIMARY KEY(id))");
            session.execute("CREATE TABLE IF NOT EXISTS my_key.my_table_by_zip (zip text, id uuid, balance int, PRIMARY KEY(zip, id))");
            //setup queries
            final PreparedStatement insert = session.prepare("INSERT INTO my_key.my_table (id, name, address, state, zip, balance) VALUES (?, ?, ?, ?, ?, ?)");
            final PreparedStatement insertRollup = session.prepare("INSERT INTO my_key.my_table_by_zip (zip, id, balance) VALUES (?, ?, ?)");
            final PreparedStatement rowLookup = session.prepare("SELECT * FROM my_key.my_table WHERE id = ?");
            final PreparedStatement rollup = session.prepare("SELECT sum(balance) FROM my_key.my_table_by_zip WHERE zip = ?");
            final List<UUID> ids = new ArrayList<>();
            final Random rnd = new Random();
            final Locale us = new Locale("en-US");
            final Faker faker = new Faker(us);
            final Supplier<UUID> getId = ()-> {
                if (ids.size() == 0){
                    //return random uuid will be record not found
                    return UUID.randomUUID();
                }
                final int itemIndex = rnd.nextInt(ids.size()-1);
                return ids.get(itemIndex);
            };
            final Supplier<Statement<?>> getOp = ()-> {
                int chance = rnd.nextInt(100);
                if (chance > 0 && chance < 50){
                    final String state = faker.address().stateAbbr();
                    final String zip = faker.address().zipCodeByState(state);
                    final UUID newId = UUID.randomUUID();
                    final int balance = rnd.nextInt();
                    ids.add(newId);
                    return BatchStatement.builder(BatchType.LOGGED)
                        .addStatement(insert.bind(newId,
                                    faker.name().fullName(), 
                                    faker.address().streetAddress(), 
                                    state, 
                                    zip,
                                    balance))
                        .addStatement(insertRollup.bind(zip, newId, balance))
                        .build();
                } else if (chance > 50 && chance < 75){
                    return rowLookup.bind(getId.get());
                } 
                final String state = faker.address().stateAbbr();
                final String zip = faker.address().zipCodeByState(state);
                return rollup.bind(zip);
            };

            List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>();
            int errorCounter = 0;
            for (int i = 0; i < trans; i++){
                //this is an uncessary hack to port old code and cap transactions in flight
                if ( i % inFlight == 0){
                    for (CompletionStage<AsyncResultSet> future: futures){
                        try{
                            future.thenRun(()->{});
                        }catch(Exception ex){
                            if (maxErrors > 0){
                                if (errorCounter > maxErrors){
                                    System.out.println("too many errors therefore stopping.");
                                    break;
                                }
                                errorCounter += 1;
                            }
                        }
                    }
                    futures = new ArrayList<>(); 
                    System.out.println("submitted " + Integer.toString(i) + " of " + Long.toString(trans) + " transactions");
               }
               Statement<?> query = getOp.get();
               futures.add(session.executeAsync(query));
            }
        }
    }
}
