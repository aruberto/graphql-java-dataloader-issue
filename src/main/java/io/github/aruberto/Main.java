package io.github.aruberto;

import static graphql.schema.idl.RuntimeWiring.newRuntimeWiring;

import com.github.javafaker.Faker;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.execution.instrumentation.dataloader.DataLoaderDispatcherInstrumentation;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.dataloader.BatchLoader;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;

public class Main {

  public static void main(String[] args) {
    Faker faker = new Faker();
    String schema =
        "type Company { "
        + "  id: Int! "
        + "  name: String! "
        + "} "
        + "type Person { "
        + "  id: Int! "
        + "  name: String! "
        + "  company: Company! "
        + "} "
        + "type Product { "
        + "  id: Int! "
        + "  name: String! "
        + "  suppliedBy: Person! "
        + "} "
        + "type QueryType { "
        + "  products: [Product!]! "
        + "} "
        + "schema { "
        + "  query: QueryType "
        + "}";

    BatchLoader<Integer, Person> personBatchLoader =
        keys -> CompletableFuture.supplyAsync(() -> {
          System.out.println("PERSON BATCH LOADER STARTING WITH " + keys.size() + " KEYS!");

          return keys.stream()
              .map(id -> new Person(id, faker.name().fullName(), id + 200))
              .collect(Collectors.toList());
        });
    BatchLoader<Integer, Company> companyBatchLoader =
        keys -> CompletableFuture.supplyAsync(() -> {
          System.out.println("COMPANY BATCH LOADER STARTING WITH " + keys.size() + " KEYS!");

          return keys.stream()
              .map(id -> new Company(id, faker.company().name()))
              .collect(Collectors.toList());
        });

    DataLoader<Integer, Person> personDataLoader = new DataLoader<>(personBatchLoader);
    DataLoader<Integer, Company> companyDataLoader = new DataLoader<>(companyBatchLoader);

    DataLoaderRegistry registry = new DataLoaderRegistry();
    registry.register("person", personDataLoader);
    registry.register("company", companyDataLoader);

    DataLoaderDispatcherInstrumentation dispatcherInstrumentation = new DataLoaderDispatcherInstrumentation(registry);

    SchemaParser schemaParser = new SchemaParser();
    TypeDefinitionRegistry typeDefinitionRegistry = schemaParser.parse(schema);

    RuntimeWiring runtimeWiring = newRuntimeWiring()
        .type("QueryType", builder -> builder
            .dataFetcher("products", environment -> {
              return IntStream.rangeClosed(0, 99)
                  .mapToObj(id -> new Product(id, faker.commerce().productName(), id + 200))
                  .collect(Collectors.toList());
            })
        )
        .type("Product", builder -> builder
              .dataFetcher("suppliedBy", environment -> {
                Product source = environment.getSource();

                return personDataLoader.load(source.getSuppliedById());
              })
        )
        .type("Person", builder -> builder
            .dataFetcher("company", environment -> {
              Person source = environment.getSource();

              return companyDataLoader.load(source.getCompanyId());
            })
        )
        .build();

    SchemaGenerator schemaGenerator = new SchemaGenerator();
    GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(typeDefinitionRegistry, runtimeWiring);

    GraphQL build = GraphQL.newGraphQL(graphQLSchema).instrumentation(dispatcherInstrumentation).build();
    ExecutionResult executionResult = build.execute(
        "query Products { "
        + "products { "
        + "id "
        + "name "
        + "suppliedBy { "
        + "id "
        + "name "
        + "company { "
        + "id "
        + "name "
        + "} "
        + "} "
        + "} "
        + "}");

    System.out.println(executionResult.getData().toString());
  }

  public static class Product {

    private final int id;
    private final String name;
    private final int suppliedById;

    public Product(int id, String name, int suppliedById) {
      this.id = id;
      this.name = name;
      this.suppliedById = suppliedById;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public int getSuppliedById() {
      return suppliedById;
    }
  }

  public static class Person {

    private final int id;
    private final String name;
    private final int companyId;

    public Person(int id, String name, int companyId) {
      this.id = id;
      this.name = name;
      this.companyId = companyId;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    public int getCompanyId() {
      return companyId;
    }
  }

  public static class Company {

    private final int id;
    private final String name;

    public Company(int id, String name) {
      this.id = id;
      this.name = name;
    }

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }
  }
}
