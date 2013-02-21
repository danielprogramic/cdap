/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.core.service;


import com.continuuity.passport.core.exceptions.*;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Component;
import com.continuuity.passport.meta.VPC;
import com.continuuity.passport.core.security.Credentials;
import com.continuuity.passport.core.status.Status;

import java.util.List;
import java.util.Map;

/**
 *  Service that orchestrates all account and vpc crud operations
 */
public interface DataManagementService {

  /**
   * Register an {@code Account} in the system. Updates underlying data stores. Generates a unique account Id
   * @param account Account information
   * @return Instance of {@code Status}
   */
  public Account registerAccount(Account account) throws AccountAlreadyExistsException;

  /**
   * Delete an {@code Account} in the system
   * @param accountId accountId to be deleted
   * @throws AccountNotFoundException on account to be deleted not found
   */
  public void deleteAccount(int accountId) throws AccountNotFoundException;

  /**
   * Confirms the registration, generates API Key
   * @param account   Instance of {@code Account}
   * @param password  Password to be stored
   */
  public void confirmRegistration(Account account, String password);

  /**
   * Register the fact that the user has downloaded the Dev suite
   * @param accountId  accountId that downloaded dev suite
   */
  public void confirmDownload(int accountId);


  /**
   * GetAccount object
   *
   * @param accountId lookup account Id of the account
   * @return Instance of {@code Account}
   */
  public Account getAccount(int accountId);


  /**
   * Get Account object from the system
   * @param emailId look up by emailId
   * @return  Instance of {@code Account}
   */
  public Account getAccount(String emailId);

  /**
   * Update account with passed Params
   *
   * @param accountId accountId
   * @param params    Map<"keyName", "value">
   */
  public void updateAccount(int accountId, Map<String, Object> params);

  /**
   * Change password for account
   * @param accountId  accountId
   * @param oldPassword old password in the system
   * @param newPassword new password in the system
   */
  public void changePassword(int accountId, String oldPassword, String newPassword);


  /**
   * Add Meta-data for VPC, updates underlying data stores and generates a VPC ID.
   * TODO: Checks for profanity keywords in vpc name and labels
   * @param accountId
   * @param vpc
   * @return Instance of {@code VPC}
   */
  public VPC addVPC(int accountId, VPC vpc);

  /**
   * Get VPC - lookup by accountId and VPCID
   * @param accountId
   * @param vpcID
   * @return Instance of {@code VPC}
   */
  public VPC getVPC(int accountId, int vpcID);

  /**
   * Delete VPC
   * @param accountId
   * @param vpcId
   * @throws VPCNotFoundException when VPC is not present in underlying data stores
   */
  public void deleteVPC(int accountId, int vpcId) throws VPCNotFoundException;


  /**
   * Get VPC list for accountID
   *
   * @param accountId accountId identifying accounts
   * @return List of {@code VPC}
   */
  public List<VPC> getVPC(int accountId);

  /**
   * Get VPC List based on the ApiKey
   *
   * @param apiKey apiKey of the account
   * @return List of {@code VPC}
   */
  public List<VPC> getVPC(String apiKey);


  /**
   * Generate a unique id to be used in activation email to enable secure (nonce based) registration process.
   * @param id accountID to be nonce
   * @return random nonce
   * TODO: note this method doesn't really belong to account/vpc CRUD. Move to a separate interface
   */
  public int getActivationNonce(int id);

  /**
   * Get id for nonce
   * @param nonce  nonce that was generated.
   * @return id
   * @throws StaleNonceException on nonce that was generated expiring in the system
   * TODO: note this method doesn't really belong to account/vpc CRUD. Move to a separate interface
   */
  public int getActivationId(int nonce) throws StaleNonceException;

  /**
   * Generate a nonce that will be used for sessions.
   * @param id  accountId to be nonced
   * @return  random nonce
   * TODO: note this method doesn't really belong to account/vpc CRUD. Move to a separate interface
   */
  public int getSessionNonce(int id);

  /**
   * Get id for nonce
   * @param nonce
   * @return account id for nonce key
   * @throws StaleNonceException on nonce that was generated expiring in the system
   * TODO: note this method doesn't really belong to account/vpc CRUD. Move to a separate interface
   */
  public int getSessionId(int nonce) throws StaleNonceException;

  /**
   * Register a component with the account- Example: register VPC, Register DataSet
   * TODO: Note: This is not implemented for initial free VPC use case
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   */
  public Status registerComponents(String accountId, Credentials credentials,
                                   Component component);

  /**
   * Unregister a {@code Component} in the system
   * TODO: Note: This is not implemented for initial free VPC use case
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   */
  public Status unRegisterComponent(String accountId, Credentials credentials,
                                    Component component);


  /**
   * TODO: Note: This is not implemented for initial free VPC use case
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   */
  public Status updateComponent(String accountId, Credentials credentials, Component component);


}
