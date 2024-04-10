package telran.probes.repo;

import org.springframework.data.mongodb.repository.MongoRepository;

import telran.accounting.model.Account;

public interface AccountProviderRepo extends MongoRepository<Account, String> {



}
