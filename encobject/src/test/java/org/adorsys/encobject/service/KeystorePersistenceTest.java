package org.adorsys.encobject.service;

import java.io.IOException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import org.adorsys.encobject.utils.TestFsBlobStoreFactory;
import org.adorsys.encobject.utils.TestKeyUtils;
import org.jclouds.blobstore.BlobStoreContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class KeystorePersistenceTest {
	private static BlobStoreContext storeContext;
	private static KeystorePersistence keystorePersistence;
	private static String container = KeystorePersistenceTest.class.getSimpleName();

	@BeforeClass
	public static void beforeClass(){
		TestKeyUtils.turnOffEncPolicy();
		storeContext = TestFsBlobStoreFactory.getTestBlobStoreContext();
		Assume.assumeNotNull(storeContext);
		keystorePersistence = new KeystorePersistence(storeContext);

	}
	
	@AfterClass
	public static void afterClass(){
		storeContext.getBlobStore().deleteContainer(container);
		storeContext.close();
	} 

	@Test
	public void testStoreKeystore() throws NoSuchAlgorithmException, CertificateException {
		String storeid = "sampleKeyStorePersistence";
		char[] storePass = "aSimplePass".toCharArray();
		KeyStore keystore = TestKeyUtils.testSecretKeystore(storeid, storePass, "mainKey", "aSimpleSecretPass".toCharArray());
		Assume.assumeNotNull(keystore);
		keystorePersistence.saveStore(keystore, TestKeyUtils.callbackHandlerBuilder(storeid, storePass).build(), container, storeid);
		Assert.assertTrue(TestFsBlobStoreFactory.existsOnFs(container, storeid));
	}
	
	@Test
	public void testLoadKeystore(){
		String container = "KeystorePersistenceTest";
		String storeid = "sampleKeyStorePersistence";
		char[] storePass = "aSimplePass".toCharArray();
		KeyStore keystore = TestKeyUtils.testSecretKeystore(storeid, storePass, "mainKey", "aSimpleSecretPass".toCharArray());
		Assume.assumeNotNull(keystore);
		try {
			keystorePersistence.saveStore(keystore, TestKeyUtils.callbackHandlerBuilder(storeid, storePass).build(), container, storeid);
		} catch (NoSuchAlgorithmException | CertificateException e) {
			Assume.assumeNoException(e);
		}
		
		KeyStore loadedKeystore = null;
		try {
			loadedKeystore = keystorePersistence.loadKeystore(container, storeid, TestKeyUtils.callbackHandlerBuilder(storeid, storePass).build());
		} catch (CertificateException | UnknownKeyStoreException | WrongKeystoreCredentialException
				| MissingKeystoreAlgorithmException | MissingKeystoreProviderException | MissingKeyAlgorithmException
				| IOException e) {
			Assume.assumeNoException(e);
		}
		Assert.assertNotNull(loadedKeystore);
		Key key = null;
		try {
			key = loadedKeystore.getKey("mainKey", "aSimpleSecretPass".toCharArray());
		} catch (UnrecoverableKeyException | KeyStoreException | NoSuchAlgorithmException e) {
			Assert.fail(e.getMessage());
		}
		Assert.assertNotNull(key);
		
	}

}
