using System;
using System.IO;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using MySql.Data.MySqlClient;

namespace MySqlBackup_AzureBlobStorage
{
	public class Function1
	{
		private static Settings _settings;

		// Timer triggered function
		[FunctionName("BackupMySql")]
		public static async Task Run([TimerTrigger("* * 2 * * *")] TimerInfo myTimer, ILogger log)
		{
			PrepSettings();

			// Backup to local file
			string file = BackupDatabaseToFile();

			// Upload file to Azure Blob Storage
			await UploadFileToBlobStorageAsync(file);

			// Clean up old backups
			await CleanupOldBackupsAsync();

			// clean up local file
			File.Delete(file);
		}

		private static string BackupDatabaseToFile()
		{
			string file = $"{Path.GetTempPath()}{DateTime.Now:yyyy-MM-dd}.mysqlbak";

			using (MySqlConnection conn = new MySqlConnection(_settings.DatabaseConnectionString))
			{
				using (MySqlCommand cmd = new MySqlCommand())
				{
					using (MySqlBackup mb = new MySqlBackup(cmd))
					{
						cmd.Connection = conn;
						conn.Open();
						mb.ExportToFile(file);
						conn.Close();
					}
				}
			}

			return file;
		}

		private static async Task CleanupOldBackupsAsync()
		{
			DateTime cutoffDate = DateTime.Now.AddDays(_settings.RetentionDays * -1);

			BlobContainerClient blobContainerClient = new BlobContainerClient(_settings.StorageAccountConnectionString, _settings.ContainerName);

			Pageable<BlobItem> blobs = blobContainerClient.GetBlobs();

			// loop all blobs & delete anything older than cutoff date
			foreach (BlobItem blobItem in blobs)
			{
				if (blobItem.Properties.LastModified > cutoffDate) continue; // all good, not expired

				string msg = $"Deleting expired : {blobItem.Name} Last modified: {blobItem.Properties.LastModified}";
				Console.WriteLine(msg);

				BlobBaseClient client = new BlockBlobClient(_settings.StorageAccountConnectionString, _settings.ContainerName, blobItem.Name);
				await client.DeleteIfExistsAsync();
			}
		}

		private static async Task<CloudBlobContainer> GetCloudBlobContainerClientAsync()
		{
			CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_settings.StorageAccountConnectionString);
			CloudBlobClient cloudBlobClient = storageAccount.CreateCloudBlobClient();
			BlobContainerClient blobContainerClient = new BlobContainerClient(_settings.StorageAccountConnectionString, _settings.ContainerName);

			await blobContainerClient.CreateIfNotExistsAsync();

			CloudBlobContainer blobContainer = cloudBlobClient.GetContainerReference(_settings.ContainerName);

			return blobContainer;
		}

		private static void PrepSettings()
		{
			_settings = new Settings
			{
				StorageAccountConnectionString = Environment.GetEnvironmentVariable("StorageAccountConnectionString"),
				ContainerName = Environment.GetEnvironmentVariable("ContainerName"),
				DatabaseConnectionString = Environment.GetEnvironmentVariable("DatabaseConnectionString"),
				RetentionDays = Convert.ToInt32(Environment.GetEnvironmentVariable("RetentionDays"))
			};
		}

		private static async Task UploadFileToBlobStorageAsync(string file)
		{
			CloudBlobContainer blobContainer = await GetCloudBlobContainerClientAsync();
			CloudBlockBlob blob = blobContainer.GetBlockBlobReference(Path.GetFileName(file));
			using (FileStream fileStream = File.OpenRead(file))
			{
				await blob.UploadFromStreamAsync(fileStream);
				await blob.SetStandardBlobTierAsync(StandardBlobTier.Archive);
			}
		}
	}
}